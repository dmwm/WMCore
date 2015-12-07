#!/usr/bin/env python
"""
_PhEDExInjectorSubscriber_

Poll the DBSBuffer database for unsubscribed datasets, and make subscriptions
associated with these datasets.

The subscription information is stored in the DBSBuffer subscriptions table and specifies the following options
for each dataset:

- site: Site to subscribe the data to
- custodial: 1 if the subscription must be custodial, non custodial otherwise
- auto_approve: 1 if the subscription should be approved automatically, request-only otherwise
- priority: Priority of the subscription, can be Low, Normal or High
- move: 1 if the subscription is a move subscription, 0 otherwise

The usual flow of operation is:

- Find unsuscribed datasets (i.e. dbsbuffer_dataset_subscription.subscribed = 0)
- Check for existing subscription in PhEDEx for such datasets, with the same
  configuration options as registered in the dataset, mark these as already subscribed
- Subscribe the unsubscribed datasets and mark them as such in the database,
  this is done according to the configuration options and aggregated to minimize
  the number of PhEDEx requests.

Additional options are:

- config.PhEDExInjector.subscribeDatasets, if False then this worker doesn't run
"""

import threading
import logging

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.Services.PhEDEx import XMLDrop
from WMCore.Services.PhEDEx.DataStructs.PhEDExDeletion import PhEDExDeletion
from WMCore.Services.PhEDEx.DataStructs.SubscriptionList import PhEDExSubscription, SubscriptionList

from WMCore.DAOFactory import DAOFactory

class PhEDExInjectorSubscriber(BaseWorkerThread):
    """
    _PhEDExInjectorSubscriber_

    Poll the DBSBuffer database and subscribe datasets as they are
    created.
    """
    def __init__(self, config, phedex, nodeMappings):
        """
        ___init___

        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        self.phedex = phedex
        self.dbsUrl = config.DBSInterface.globalDBSUrl
        self.group = getattr(config.PhEDExInjector, "group", "DataOps")

        self.phedexNodes = {'MSS':[], 'Disk':[]}
        for node in nodeMappings["phedex"]["node"]:
            if node["kind"] in [ "MSS", "Disk" ]:
                self.phedexNodes[node["kind"]].append(node["name"])

        # initialize the alert framework (if available - config.Alert present)
        #    self.sendAlert will be then be available
        self.initAlerts(compName = "PhEDExInjector")

        return

    def setup(self, parameters):
        """
        _setup_

        Create a DAO Factory for the PhEDExInjector.  Also load the SE names to
        PhEDEx node name mappings from the data service.
        """
        myThread = threading.currentThread()
        daofactory = DAOFactory(package = "WMComponent.PhEDExInjector.Database",
                                logger = self.logger,
                                dbinterface = myThread.dbi)

        self.findDeletableBlocks = daofactory(classname = "GetDeletableBlocks")
        self.markBlocksDeleted = daofactory(classname = "MarkBlocksDeleted")
        self.getUnsubscribed = daofactory(classname = "GetUnsubscribedDatasets")
        self.markSubscribed = daofactory(classname = "MarkDatasetSubscribed")

        return

    def algorithm(self, parameters):
        """
        _algorithm_

        Run the subscription algorithm as configured
        """
        self.deleteBlocks()
        self.subscribeDatasets()
        return

    def deleteBlocks(self):
        """
        _deleteBlocks_

        Find deletable blocks, then decide if to delete based on:

        Is there an active subscription for dataset or block ?
          If yes => set deleted=2
          If no => next check

        Has transfer to all destinations finished ?
          If yes => request block deletion, approve request, set deleted=1
          If no => do nothing (check again next cycle)

        """
        logging.info("Starting deleteBlocks method")

        blockDict = self.findDeletableBlocks.execute(transaction = False)

        if len(blockDict) == 0:
            return

        subscriptions = self.phedex.getSubscriptionMapping(*blockDict.keys())

        skippableBlocks = []
        deletableEntries = {}
        for blockName in blockDict:

            location = blockDict[blockName]['location']

            # should never be triggered, better safe than sorry
            if location.endswith('_MSS'):
                logging.debug("Location %s for block %s is MSS, skip deletion", location, blockName)
                skippableBlocks.append(blockName)
                continue

            dataset = blockDict[blockName]['dataset']
            sites = blockDict[blockName]['sites']

            if blockName in subscriptions and location in subscriptions[blockName]:
                logging.debug("Block %s subscribed to %s, skip deletion",  blockName, location)
                binds = { 'DELETED' : 2,
                          'BLOCKNAME' : blockName }
                self.markBlocksDeleted.execute(binds, transaction = False)
            else:
                blockInfo = []
                try:
                    blockInfo = self.phedex.getReplicaInfoForBlocks(block = blockName, complete = 'y')['phedex']['block']
                except:
                    logging.error("Couldn't get block info from PhEDEx, retry next cycle")
                for entry in blockInfo:
                    if entry['name'] == blockName:
                        nodes = set([x['node'] for x in entry['replica']])
                        if location not in nodes:
                            logging.debug("Block %s not present on %s, mark as deleted", blockName, location)
                            binds = { 'DELETED' : 1,
                                      'BLOCKNAME' : blockName }
                            self.markBlocksDeleted.execute(binds, transaction = False)
                        elif sites.issubset(nodes):
                            logging.debug("Deleting block %s from %s since it is fully transfered", blockName, location)
                            if location not in deletableEntries:
                                deletableEntries[location] = {}
                            if dataset not in deletableEntries[location]:
                                deletableEntries[location][dataset] = set()
                            deletableEntries[location][dataset].add(blockName)


        binds = []
        for blockName in skippableBlocks:
            binds.append( { 'DELETED' : 2,
                            'BLOCKNAME' : blockName } )
        if len(binds) > 0:
            self.markBlocksDeleted.execute(binds, transaction = False)

        for location in deletableEntries:

            chunkSize = 100
            numberOfBlocks = 0
            blocksToDelete = {}
            for dataset in deletableEntries[location]:

                blocksToDelete[dataset] = deletableEntries[location][dataset]
                numberOfBlocks += len(blocksToDelete[dataset])

                if numberOfBlocks > chunkSize:

                    self.deleteBlocksPhEDExCalls(location, blocksToDelete)
                    numberOfBlocks = 0
                    blocksToDelete = {}

            self.deleteBlocksPhEDExCalls(location, blocksToDelete)

        return

    def deleteBlocksPhEDExCalls(self, location, blocksToDelete):
        """
        _deleteBlocksPhEDExCalls_

        actual PhEDEx calls for block deletion
        """
        deletion = PhEDExDeletion(blocksToDelete.keys(), location,
                                  level = 'block',
                                  comments = "WMAgent blocks auto-delete from %s" % location,
                                  blocks = blocksToDelete)

        try:
            xmlData = XMLDrop.makePhEDExXMLForBlocks(self.dbsUrl,
                                                     deletion.getDatasetsAndBlocks())
            logging.debug(str(xmlData))
            response = self.phedex.delete(deletion, xmlData)
            requestId = response['phedex']['request_created'][0]['id']

            # auto-approve deletion request
            self.phedex.updateRequest(requestId, 'approve', location)

            binds = []
            for dataset in blocksToDelete:
                for blockName in blocksToDelete[dataset]:
                    binds.append( { 'DELETED' : 1,
                                    'BLOCKNAME' : blockName } )

            self.markBlocksDeleted.execute(binds, transaction = False)

        except Exception as ex:
            logging.error("Something went wrong when communicating with PhEDEx, will try again later.")
            logging.error("Exception: %s", str(ex))

        return


    def subscribeDatasets(self):
        """
        _subscribeDatasets_

        Poll the database for datasets and subscribe them.
        """
        logging.info("Starting subscribeDatasets method")

        myThread = threading.currentThread()
        myThread.transaction.begin()

        # Check for completely unsubscribed datasets
        unsubscribedDatasets = self.getUnsubscribed.execute(conn = myThread.transaction.conn,
                                                            transaction = True)

        # Keep a list of subscriptions to tick as subscribed in the database
        subscriptionsMade = []

        # Create a list of subscriptions as defined by the PhEDEx data structures
        subs = SubscriptionList()

        # Create the subscription objects and add them to the list
        # The list takes care of the sorting internally
        for subInfo in unsubscribedDatasets:
            site = subInfo['site']

            if site not in self.phedexNodes['MSS'] and site not in self.phedexNodes['Disk']:
                msg = "Site %s doesn't appear to be valid to PhEDEx, " % site
                msg += "skipping subscription: %s" % subInfo['id']
                logging.error(msg)
                self.sendAlert(7, msg = msg)
                continue

            # Avoid custodial subscriptions to disk nodes
            if site not in self.phedexNodes['MSS']: 
                subInfo['custodial'] = 'n'
            # Avoid auto approval in T1 sites
            elif site.startswith("T1"):
                subInfo['request_only'] = 'y'
            
            phedexSub = PhEDExSubscription(subInfo['path'], site,
                                           self.group, priority = subInfo['priority'],
                                           move = subInfo['move'], custodial = subInfo['custodial'],
                                           request_only = subInfo['request_only'], subscriptionId = subInfo['id'])

            # Check if the subscription is a duplicate
            if phedexSub.matchesExistingSubscription(self.phedex) or \
                phedexSub.matchesExistingTransferRequest(self.phedex):
                subscriptionsMade.append(subInfo['id'])
                continue

            # Add it to the list
            subs.addSubscription(phedexSub)

        # Compact the subscriptions
        subs.compact()

        for subscription in subs.getSubscriptionList():
            try:
                xmlData = XMLDrop.makePhEDExXMLForDatasets(self.dbsUrl,
                                                           subscription.getDatasetPaths())
                logging.debug(str(xmlData))
                msg = "Subscribing: %s to %s, with options: " % (subscription.getDatasetPaths(), subscription.getNodes())
                msg += "Move: %s, Custodial: %s, Request Only: %s" % (subscription.move, subscription.custodial, subscription.request_only)
                logging.info(msg)
                self.phedex.subscribe(subscription, xmlData)
            except Exception as ex:
                logging.error("Something went wrong when communicating with PhEDEx, will try again later.")
                logging.error("Exception: %s", str(ex))
            else:
                subscriptionsMade.extend(subscription.getSubscriptionIds())

        # Register the result in DBSBuffer
        if subscriptionsMade:
            self.markSubscribed.execute(subscriptionsMade,
                                        conn = myThread.transaction.conn,
                                        transaction = True)

        myThread.transaction.commit()
        return
