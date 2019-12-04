#!/usr/bin/env python
"""
_PhEDExInjectorPoller_

This component handles the WMAgent interactions with PhEDEx, which falls into 4 categories
 - injecting files
 - closing blocks
 - making subscriptions
 - deleting blocks (for copy+delete subscriptions)

Making subscriptions and deleting blocks is optional, triggered by the subscribeDatasets parameter.
Even if they are enabled, they'll run at a different (longer) intervall than the main polling loop.

File injection and block closing runs most frequently, the latter is just following DBS block closing.

For subscription making, we poll the DBSBuffer database for unsubscribed datasets and make subscriptions
associated with these datasets.

The subscription information is stored in the DBSBuffer subscriptions table and specifies the following options
for each dataset:
- site: Site to subscribe the data to
- custodial: 1 if the subscription must be custodial, non custodial otherwise
- auto_approve: 1 if the subscription should be approved automatically, request-only otherwise
- priority: Priority of the subscription, can be Low, Normal or High
- move: 1 if the subscription is a move subscription, 0 otherwise
- phedex_group: what PhEDEx group the subscription should be made with
- delete_blocks: whether or not the blocks should be deleted after they are 'finished'

The usual flow of operation is:
- Find unsuscribed datasets (i.e. dbsbuffer_dataset_subscription.subscribed = 0)
- Check for existing subscription in PhEDEx for such datasets, with the same
  configuration options as registered in the dataset, mark these as already subscribed
- Subscribe the unsubscribed datasets and mark them as such in the database,
  this is done according to the configuration options and aggregated to minimize
  the number of PhEDEx requests.

Block deletion triggers on processing being done for all workflows that created files
in a block, the block being fully transferred to all target sites and no subscription
being present for the site we injected it at. If all these conditions are met, it'll be
deleted from the site it was originally injected at.

"""

import logging
import threading
import time
import traceback
from httplib import HTTPException

from Utils.Timers import timeFunction
from WMCore.DAOFactory import DAOFactory
from WMCore.Services.PhEDEx import XMLDrop
from WMCore.Services.PhEDEx.DataStructs.PhEDExDeletion import PhEDExDeletion
from WMCore.Services.PhEDEx.DataStructs.SubscriptionList import PhEDExSubscription, SubscriptionList
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.WMException import WMException
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread


class PhEDExInjectorException(WMException):
    """
    _PhEDExInjectorException_

    Specific PhEDExInjectorPoller exception handling.
    """


class PhEDExInjectorPoller(BaseWorkerThread):
    """
    _PhEDExInjectorPoller_

    Poll the DBSBuffer database and inject files as they are created.
    """

    def __init__(self, config):
        """
        ___init___

        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        self.dbsUrl = config.DBSInterface.globalDBSUrl

        self.pollCounter = 0
        self.subFrequency = None
        if getattr(config.PhEDExInjector, "subscribeDatasets", False):
            pollInterval = config.PhEDExInjector.pollInterval
            subInterval = config.PhEDExInjector.subscribeInterval
            self.subFrequency = max(1, int(round(subInterval / pollInterval)))
            logging.info("SubscribeDataset and deleteBlocks will run every %d polling cycles", self.subFrequency)
            # subscribe on first cycle
            self.pollCounter = self.subFrequency - 1

        # retrieving the node mappings is fickle and can fail quite often
        self.phedex = PhEDEx({"endpoint": config.PhEDExInjector.phedexurl},
                             "json", dbsUrl=self.dbsUrl)
        try:
            nodeMappings = self.phedex.getNodeMap()
        except:
            time.sleep(2)
            try:
                nodeMappings = self.phedex.getNodeMap()
            except:
                time.sleep(4)
                nodeMappings = self.phedex.getNodeMap()

        # This will be used to map SE names which are stored in the DBSBuffer to
        # PhEDEx node names.  The first key will be the "kind" which consists
        # of one of the following: MSS, Disk, Buffer.  The next key will be the
        # SE name.
        self.seMap = {}
        self.nodeNames = []
        for node in nodeMappings["phedex"]["node"]:
            if node["kind"] not in self.seMap:
                self.seMap[node["kind"]] = {}
            logging.info("Adding mapping %s -> %s", node["se"], node["name"])
            self.seMap[node["kind"]][node["se"]] = node["name"]
            self.nodeNames.append(node["name"])

        self.phedexNodes = {'MSS': [], 'Disk': []}
        for node in nodeMappings["phedex"]["node"]:
            if node["kind"] in ["MSS", "Disk"]:
                self.phedexNodes[node["kind"]].append(node["name"])

        self.blocksToRecover = []

        return

    def setup(self, parameters):
        """
        _setup_

        Create DAO Factory and setup some DAO.
        """
        myThread = threading.currentThread()
        daofactory = DAOFactory(package="WMComponent.PhEDExInjector.Database",
                                logger=self.logger, dbinterface=myThread.dbi)

        self.getUninjected = daofactory(classname="GetUninjectedFiles")
        self.getMigrated = daofactory(classname="GetMigratedBlocks")

        self.findDeletableBlocks = daofactory(classname="GetDeletableBlocks")
        self.markBlocksDeleted = daofactory(classname="MarkBlocksDeleted")
        self.getUnsubscribed = daofactory(classname="GetUnsubscribedDatasets")
        self.markSubscribed = daofactory(classname="MarkDatasetSubscribed")

        daofactory = DAOFactory(package="WMComponent.DBS3Buffer",
                                logger=self.logger, dbinterface=myThread.dbi)
        self.setStatus = daofactory(classname="DBSBufferFiles.SetPhEDExStatus")
        self.setBlockClosed = daofactory(classname="SetBlockClosed")

        return

    @timeFunction
    def algorithm(self, parameters):
        """
        _algorithm_

        Poll the database for uninjected files and attempt to inject them into
        PhEDEx.
        """
        logging.info("Running PhEDEx injector poller algorithm...")
        self.pollCounter += 1

        try:
            if self.blocksToRecover:
                logging.info("""PhEDExInjector Recovery:
                                previous injection call failed,
                                checking if files were injected to PhEDEx anyway""")
                self.recoverInjectedFiles()

            self.injectFiles()
            self.closeBlocks()

            if self.pollCounter == self.subFrequency:
                self.pollCounter = 0
                self.deleteBlocks()
                self.subscribeDatasets()
        except HTTPException as ex:
            if hasattr(ex, "status") and ex.status in [502, 503]:
                # then either proxy error or service is unavailable
                msg = "Caught HTTPException in PhEDExInjector. Retrying in the next cycle.\n"
                msg += str(ex)
                logging.error(msg)
            else:
                msg = "Caught unexpected HTTPException in PhEDExInjector.\n%s" % str(ex)
                logging.exception(msg)
                raise
        except Exception as ex:
            msg = "Caught unexpected exception in PhEDExInjector. Details:\n%s" % str(ex)
            logging.exception(msg)
            raise PhEDExInjectorException(msg)

        return

    def createInjectionSpec(self, injectionData):
        """
        _createInjectionSpec_

        Transform the data structure returned from the database into an XML
        string for the PhEDEx Data Service.  The injectionData parameter must be
        a dictionary keyed by dataset path.  Each dataset path will map to a
        list of blocks, each block being a dict.  The block dicts will have
        three keys: name, is-open and files.  The files key will be a list of
        dicts, each of which have the following keys: lfn, size and checksum.
        The following is an example object:

        {"dataset1":
          {"block1": {"is-open": "y", "files":
            [{"lfn": "lfn1", "size": 10, "checksum": {"cksum": "1234"}},
             {"lfn": "lfn2", "size": 20, "checksum": {"cksum": "4321"}}]}}}
        """
        injectionSpec = XMLDrop.XMLInjectionSpec(self.dbsUrl)

        for datasetPath in injectionData:
            datasetSpec = injectionSpec.getDataset(datasetPath)

            for fileBlockName, fileBlock in injectionData[datasetPath].iteritems():
                blockSpec = datasetSpec.getFileblock(fileBlockName,
                                                     fileBlock["is-open"])

                for f in fileBlock["files"]:
                    blockSpec.addFile(f["lfn"], f["checksum"], f["size"])

        return injectionSpec.save()

    def createRecoveryFileFormat(self, unInjectedData):
        """
        _createRecoveryFileFormat_

        Transform the data structure returned from database in to the dict format
        for the PhEDEx Data Service.  The injectionData parameter must be
        a dictionary keyed by dataset path.

        unInjectedData format
        {"dataset1":
          {"block1": {"is-open": "y", "files":
            [{"lfn": "lfn1", "size": 10, "checksum": {"cksum": "1234"}},
             {"lfn": "lfn2", "size": 20, "checksum": {"cksum": "4321"}}]}}}

        returns
        [{"block1": set(["lfn1", "lfn2"])}, {"block2": set(["lfn3", "lfn4"])]
        """
        blocks = []
        for datasetPath in unInjectedData:

            for blockName, fileBlock in unInjectedData[datasetPath].items():

                newBlock = {blockName: set()}

                for fileDict in fileBlock["files"]:
                    newBlock[blockName].add(fileDict["lfn"])

                blocks.append(newBlock)

        return blocks

    def injectFiles(self):
        """
        _injectFiles_

        Inject any uninjected files in PhEDEx.
        """
        logging.info("Starting injectFiles method")

        uninjectedFiles = self.getUninjected.execute()

        for siteName in uninjectedFiles.keys():
            # SE names can be stored in DBSBuffer as that is what is returned in
            # the framework job report.  We'll try to map the SE name to a
            # PhEDEx node name here.
            location = None

            if siteName in self.nodeNames:
                location = siteName
            else:
                if "Buffer" in self.seMap and siteName in self.seMap["Buffer"]:
                    location = self.seMap["Buffer"][siteName]
                elif "MSS" in self.seMap and siteName in self.seMap["MSS"]:
                    location = self.seMap["MSS"][siteName]
                elif "Disk" in self.seMap and siteName in self.seMap["Disk"]:
                    location = self.seMap["Disk"][siteName]

            if location is None:
                msg = "Could not map SE %s to PhEDEx node." % siteName
                logging.error(msg)
                continue

            for dataset in uninjectedFiles[siteName]:
                injectData = {}
                lfnList = []
                injectData[dataset] = uninjectedFiles[siteName][dataset]

                for block in injectData[dataset]:
                    for fileInfo in injectData[dataset][block]['files']:
                        lfnList.append(fileInfo['lfn'])
                    logging.info("About to inject %d files for block %s",
                                 len(injectData[dataset][block]['files']), block)

                self.injectFilesPhEDExCall(location, injectData, lfnList)

        return

    def injectFilesPhEDExCall(self, location, injectData, lfnList):
        """
        _injectFilesPhEDExCall_

        actual PhEDEx call for file injection
        """
        xmlData = self.createInjectionSpec(injectData)
        logging.debug("injectFiles XMLData: %s", xmlData)

        try:
            injectRes = self.phedex.injectBlocks(location, xmlData)
        except HTTPException as ex:
            # HTTPException with status 400 assumed to be duplicate injection
            # trigger later block recovery (investigation needed if not the case)
            if ex.status == 400:
                self.blocksToRecover.extend(self.createRecoveryFileFormat(injectData))
            logging.error("PhEDEx file injection failed with HTTPException: %s %s", ex.status, ex.result)
        except Exception as ex:
            msg = "PhEDEx file injection failed with Exception: %s" % str(ex)
            logging.exception(msg)
        else:
            logging.debug("Injection result: %s", injectRes)

            if "error" in injectRes:
                msg = "Error injecting data %s: %s" % (injectData, injectRes["error"])
                logging.error(msg)
            else:
                try:
                    self.setStatus.execute(lfnList, 1)
                except Exception as ex:
                    if 'Deadlock found' in str(ex) or 'deadlock detected' in str(ex):
                        logging.error("Database deadlock during file status update. Retrying again in the next cycle.")
                        self.blocksToRecover.extend(self.createRecoveryFileFormat(injectData))
                    else:
                        msg = "Failed to update file status in the database, reason: %s" % str(ex)
                        logging.error(msg)
                        raise PhEDExInjectorException(msg)

        return

    def closeBlocks(self):
        """
        _closeBlocks_

        Close any blocks that have been migrated to global DBS
        """
        logging.info("Starting closeBlocks method")

        migratedBlocks = self.getMigrated.execute()

        for siteName in migratedBlocks:
            # SE names can be stored in DBSBuffer as that is what is returned in
            # the framework job report.  We'll try to map the SE name to a
            # PhEDEx node name here.
            location = None

            if siteName in self.nodeNames:
                location = siteName
            else:
                if "Buffer" in self.seMap and siteName in self.seMap["Buffer"]:
                    location = self.seMap["Buffer"][siteName]
                elif "MSS" in self.seMap and siteName in self.seMap["MSS"]:
                    location = self.seMap["MSS"][siteName]
                elif "Disk" in self.seMap and siteName in self.seMap["Disk"]:
                    location = self.seMap["Disk"][siteName]

            if location is None:
                msg = "Could not map SE %s to PhEDEx node." % siteName
                logging.error(msg)
                continue

            for dset, blocks in migratedBlocks[siteName].items():
                xmlData = self.createInjectionSpec({dset: blocks})
                logging.debug("closeBlocks XMLData: %s", xmlData)

                try:
                    injectRes = self.phedex.injectBlocks(location, xmlData)
                except HTTPException as ex:
                    logging.error("PhEDEx block close failed with HTTPException: %s %s", ex.status, ex.result)
                except Exception as ex:
                    msg = "PhEDEx block close failed with Exception: %s" % str(ex)
                    logging.exception(msg)
                else:
                    logging.debug("Block closing result: %s", injectRes)

                    if "error" in injectRes:
                        logging.error("Failed to close blocks due to: %s, for data: %s",
                                      injectRes["error"], migratedBlocks[siteName][dset])
                    else:
                        for blockName in blocks:
                            logging.info("Block closed in PhEDEx: %s", blockName)
                            self.setBlockClosed.execute(blockName)

        return

    def recoverInjectedFiles(self):
        """
        When PhEDEx inject call timed out, run this function.
        Since there are 3 min reponse time out in cmsweb, some times
        PhEDEx injection call times out even though the call succeeded
        In that case run the recovery mode
        1. first check whether files which injection status = 0 are in the PhEDEx.
        2. if those file exist set the in_phedex status to 1
        3. set self.blocksToRecover = []

        Run this recovery one block at a time, with too many blocks
        the call to the PhEDEx data service on cmsweb can time out
        """
        # recover one block at a time
        for block in self.blocksToRecover:

            injectedFiles = self.phedex.getInjectedFiles(block)

            if injectedFiles:
                self.setStatus.execute(injectedFiles, 1)

        self.blocksToRecover = []
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

        blockDict = self.findDeletableBlocks.execute(transaction=False)

        if not blockDict:
            return

        try:
            subscriptions = self.phedex.getSubscriptionMapping(*blockDict.keys())
        except:
            logging.error("Couldn't get subscription info from PhEDEx, retry next cycle")
            return

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
                logging.debug("Block %s subscribed to %s, skip deletion", blockName, location)
                binds = {'DELETED': 2, 'BLOCKNAME': blockName}
                self.markBlocksDeleted.execute(binds)
            else:
                blockInfo = []
                try:
                    blockInfo = self.phedex.getReplicaInfoForBlocks(block=blockName, complete='y')['phedex']['block']
                except:
                    logging.error("Couldn't get block info from PhEDEx, retry next cycle")
                else:
                    nodes = set()
                    for entry in blockInfo:
                        if entry['name'] == blockName:
                            nodes = set([x['node'] for x in entry['replica']])
                    if location not in nodes:
                        logging.debug("Block %s not present on %s, mark as deleted", blockName, location)
                        binds = {'DELETED': 1, 'BLOCKNAME': blockName}
                        self.markBlocksDeleted.execute(binds)
                    elif sites.issubset(nodes):
                        logging.debug("Deleting block %s from %s since it is fully transfered", blockName, location)
                        if location not in deletableEntries:
                            deletableEntries[location] = {}
                        if dataset not in deletableEntries[location]:
                            deletableEntries[location][dataset] = set()
                        deletableEntries[location][dataset].add(blockName)

        binds = []
        for blockName in skippableBlocks:
            binds.append({'DELETED': 2, 'BLOCKNAME': blockName})
        if binds:
            self.markBlocksDeleted.execute(binds)

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
                                  level='block',
                                  comments="WMAgent blocks auto-delete from %s" % location,
                                  blocks=blocksToDelete)

        try:
            response = self.phedex.delete(deletion)
            requestId = response['phedex']['request_created'][0]['id']
            # auto-approve deletion request
            self.phedex.updateRequest(requestId, 'approve', location)
        except HTTPException as ex:
            logging.error("PhEDEx block delete/approval failed with HTTPException: %s %s", ex.status, ex.result)
        except Exception as ex:
            logging.error("PhEDEx block delete/approval failed with Exception: %s", str(ex))
            logging.debug("Traceback: %s", str(traceback.format_exc()))
        else:
            binds = []
            for dataset in blocksToDelete:
                for blockName in blocksToDelete[dataset]:
                    binds.append({'DELETED': 1,
                                  'BLOCKNAME': blockName})
            self.markBlocksDeleted.execute(binds)

        return

    def subscribeDatasets(self):
        """
        _subscribeDatasets_
        Poll the database for datasets and subscribe them.
        """
        logging.info("Starting subscribeDatasets method")

        # Check for completely unsubscribed datasets
        unsubscribedDatasets = self.getUnsubscribed.execute()

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
                continue

            # Avoid custodial subscriptions to disk nodes
            if site not in self.phedexNodes['MSS']:
                subInfo['custodial'] = 'n'
            # Avoid auto approval in T1 sites
            elif site.startswith("T1"):
                subInfo['request_only'] = 'y'

            phedexSub = PhEDExSubscription(subInfo['path'], site, subInfo['phedex_group'],
                                           priority=subInfo['priority'],
                                           move=subInfo['move'],
                                           custodial=subInfo['custodial'],
                                           request_only=subInfo['request_only'],
                                           subscriptionId=subInfo['id'])

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

            logging.info("Subscribing: %s to %s, with options: Move: %s, Custodial: %s, Request Only: %s",
                         subscription.getDatasetPaths(),
                         subscription.getNodes(),
                         subscription.move,
                         subscription.custodial,
                         subscription.request_only)

            try:
                self.phedex.subscribe(subscription)
            except HTTPException as ex:
                logging.error("PhEDEx dataset subscribe failed with HTTPException: %s %s", ex.status, ex.result)
            except Exception as ex:
                logging.error("PhEDEx dataset subscribe failed with Exception: %s", str(ex))
                logging.debug("Traceback: %s", str(traceback.format_exc()))
            else:
                subscriptionsMade.extend(subscription.getSubscriptionIds())

        # Register the result in DBSBuffer
        if subscriptionsMade:
            self.markSubscribed.execute(subscriptionsMade)

        return
