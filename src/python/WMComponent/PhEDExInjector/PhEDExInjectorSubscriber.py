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

There is a Tier-0 operation mode:

- Look for dataset subscriptions to the Tier-0, even if already marked as subscribed.
- Find all closed blocks belonging to the Tier-0 datasets which don't contain files
  produced by active workflows, where an active workflow is defined as a workflow
  still present in WMBS
- Subscribe those blocks as move custodial auto-approved

The Tier-0 mode is activated using config.PhEDExInjector.tier0Mode = True

Additional options are:

- config.PhEDExInjector.subscribeDatasets, if False then this worker doesn't run
"""

import logging
import threading
import time

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.Services.PhEDEx import XMLDrop
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Services.PhEDEx.DataStructs.PhEDExDeletion import PhEDExDeletion
from WMCore.Services.PhEDEx.DataStructs.SubscriptionList import PhEDExSubscription, SubscriptionList
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON

from WMCore.DAOFactory import DAOFactory

class PhEDExInjectorSubscriber(BaseWorkerThread):
    """
    _PhEDExInjectorSubscriber_

    Poll the DBSBuffer database and subscribe datasets as they are
    created.
    """
    def __init__(self, config):
        """
        ___init___

        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        self.phedex = PhEDEx({"endpoint": config.PhEDExInjector.phedexurl}, "json")
        self.siteDB = SiteDBJSON()
        self.dbsUrl = config.DBSInterface.globalDBSUrl
        self.group = getattr(config.PhEDExInjector, "group", "DataOps")
        self.tier0Mode = getattr(config.PhEDExInjector, "tier0Mode", False)
        self.blockDeletionTimespan = getattr(config.PhEDExInjector, "blockTimeout", 7 * 24 * 3600)
        self.autoDelete = getattr(config.PhEDExInjector, "autoDelete", True)

        # We will map node names to CMS names, that what the spec will have.
        # If a CMS name is associated to many PhEDEx node then choose the MSS option
        self.cmsToPhedexMap = {}        
        self.phedexNodes = {'MSS':[], 'Disk':[]}

        # initialize the alert framework (if available - config.Alert present)
        #    self.sendAlert will be then be available
        self.initAlerts(compName = "PhEDExInjector")


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

        self.getUnsubscribed = daofactory(classname = "GetUnsubscribedDatasets")
        self.getSubscribedBlocks = daofactory(classname = "GetSubscribedBlocks")
        self.markSubscribed = daofactory(classname = "MarkDatasetSubscribed")

        nodeMappings = self.phedex.getNodeMap()
        for node in nodeMappings["phedex"]["node"]:

            cmsName = self.siteDB.phEDExNodetocmsName(node["name"])

            if cmsName not in self.cmsToPhedexMap:
                self.cmsToPhedexMap[cmsName] = {}

            logging.info("Loaded PhEDEx node %s for site %s" % (node["name"], cmsName))
            if node["kind"] not in self.cmsToPhedexMap[cmsName]:
                self.cmsToPhedexMap[cmsName][node["kind"]] = node["name"]

            if node["kind"] in [ "MSS", "Disk" ]:
                self.phedexNodes[node["kind"]].append(node["name"])
        return

    def algorithm(self, parameters):
        """
        _algorithm_

        Run the subscription algorithm as configured
        """
        if self.tier0Mode:
            self.deleteTier0Blocks()
        self.subscribeDatasets()
        return

    def deleteTier0Blocks(self):
        """
        _deleteTier0Blocks_

        Check blocks on Disk nodes that
         - have at least one subscription to an MSS node
         - the block has finished transfer for all of MSS subscriptions
         - the workflow producing the block is complete and was cleaned up

        All such blocks are then deleted. As this is an ever growing
        query it can be configured to consider only blocks created
        after certain date (default 1 week).
        """
        # Check for candidate blocks for subscription
        candidateBlocks = self.getSubscribedBlocks.execute(timeout = self.blockDeletionTimespan)

        if not candidateBlocks:
            return

        # We got the blocks, sort them by dataset.
        blocksByDataset = {}
        for entry in candidateBlocks:
            dataset = entry['path']
            if dataset not in blocksByDataset:
                blocksByDataset[dataset] = set()
            blocksByDataset[dataset].add(entry['blockname'])

        # Check for dataset subscriptions to
        # T2_CH_CERN and remove those datasets from the mix. Then check for blocks already
        # transferred to a custodial site, these are the blocks to delete.
        subscriptions = self.phedex.getSubscriptionMapping(*blocksByDataset.keys())
        deletableEntries = {}
        for dataset in blocksByDataset:
            if dataset in subscriptions and "T2_CH_CERN" in subscriptions[dataset]:
                # Ignore all the blocks for this dataset
                logging.debug("Dataset %s is subscribed to T2_CH_CERN" % dataset)
                continue
            # Call block replicas per dataset instead of per block
            blockInfo = self.phedex.getReplicaInfoForBlocks(dataset = dataset, custodial = 'y', complete = 'y')['phedex']['block']
            for entry in blockInfo:
                if entry['name'] in blocksByDataset[dataset]:
                    if dataset not in deletableEntries:
                        deletableEntries[dataset] = set()
                    nodes = [x['node'] for x in entry['replica']]
                    logging.debug("Deleting block %s from T2_CH_CERN as it is already transferred to %s" % (entry['name'], ', '.join(nodes)))
                    deletableEntries[dataset].add(entry['name'])

        if not deletableEntries:
            return

        deletion = PhEDExDeletion(deletableEntries.keys(),
                                  'T2_CH_CERN',
                                  level = 'block',
                                  comments = 'Blocks automatically deleted from T2_CH_CERN as it has already been processed and transferred to a custodial location',
                                  blocks = deletableEntries)

        try:
            xmlData = XMLDrop.makePhEDExXMLForBlocks(self.dbsUrl,
                                                     deletion.getDatasetsAndBlocks())
            logging.debug(str(xmlData))
            response = self.phedex.delete(deletion, xmlData)
            requestId = response['phedex']['request_created'][0]['id']
            # Auto-approve the request unless noted otherwise
            if self.autoDelete:
                self.phedex.updateRequest(requestId, 'approve', 'T2_CH_CERN')
                logging.info("Deleted %d blocks from T2_CH_CERN" % sum([len(x) for x in deletion.getDatasetsAndBlocks().values()]))
            else:
                logging.info("Created deletion requests for %d blocks from T2_CH_CERN" % sum([len(x) for x in deletion.getDatasetsAndBlocks().values()]))
        except Exception, ex:
            logging.error("Something went wrong when communicating with PhEDEx, will try again later.")
            logging.error("Exception: %s" % str(ex))

    def subscribeDatasets(self):
        """
        _subscribeDatasets_

        Poll the database for datasets and subscribe them.
        """
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

                if site not in self.cmsToPhedexMap:
                    msg = "Site %s doesn't appear to be valid to PhEDEx, " % site
                    msg += "skipping subscription: %s" % subInfo['id']
                    logging.error(msg)
                    self.sendAlert(7, msg = msg)
                    continue

                # Get the phedex node from CMS site
                site = self.cmsToPhedexMap[site].get("MSS") or self.cmsToPhedexMap[site]["Disk"] 

            # Avoid custodial subscriptions to disk nodes
            if site not in self.phedexNodes['MSS']: subInfo['custodial'] = 'n'
            # Avoid move subscriptions and replica
            if subInfo['custodial'] == 'n': subInfo['move'] = 'n'
           
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
            except Exception, ex:
                logging.error("Something went wrong when communicating with PhEDEx, will try again later.")
                logging.error("Exception: %s" % str(ex))
            else:
                subscriptionsMade.extend(subscription.getSubscriptionIds())

        # Register the result in DBSBuffer
        if subscriptionsMade:
            self.markSubscribed.execute(subscriptionsMade,
                                        conn = myThread.transaction.conn,
                                        transaction = True)

        myThread.transaction.commit()
        return
