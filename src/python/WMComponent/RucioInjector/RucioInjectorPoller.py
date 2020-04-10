#!/usr/bin/env python
"""
_RucioInjectorPoller_

General reminder about PhEDEx and Rucio definitions:
* CMS dataset is equivalent to a Rucio container
* CMS block is equivalent to a Rucio dataset
* CMS file is equivalent to a Rucio file
* A Rucio replica is a file, under a given scope, at a given RSE
"""
from __future__ import division

import json
import logging
import threading
import time

from Utils.MemoryCache import MemoryCache
from Utils.Timers import timeFunction
from WMCore.DAOFactory import DAOFactory
from WMCore.Services.Rucio.Rucio import Rucio
from WMCore.WMException import WMException
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread


### TODO: remove this function once PhEDExInjector is out of the game
def filterDataByTier(rawData, allowedTiers):
    """
    This function will receive data - in the same format as returned from
    the DAO - and it will pop out anything that the component is not meant
    to inject into Rucio.
    :param rawData: the large dict of location/container/block/files
    :param allowedTiers: a list of datatiers that we want to inject
    :return: the same dictionary as in the input, but without dataset structs
             for datatiers that we do not want to be processed by this component.
    """
    for location in rawData:
        for container in list(rawData[location]):
            endTier = container.rsplit('/', 1)[1]
            if endTier not in allowedTiers:
                logging.info("Container %s not meant to be injected by RucioInjector", container)
                rawData[location].pop(container)
    return rawData


class RucioInjectorException(WMException):
    """
    _RucioInjectorException_

    Specific RucioInjectorPoller exception handling.
    """


class RucioInjectorPoller(BaseWorkerThread):
    """
    _RucioInjectorPoller_

    Poll the DBSBuffer database and inject files as they are created.

    The logic of this component is:
      * create a rucio container (or reuse a pre-existent one)
      * create a CMS block (or reuse a pre-existent one), block gets automatically attached
      * create file/replicas, which get automatically attached to its block as well
      * now create a CMS block rule to protect this data
      * if the block has been inserted into DBS, close the block in Rucio

    In addition to that, it has logic for rucio container subscription (rule creation),
    and block rule removal. Those follow a different polling cycle though.
    """

    def __init__(self, config):
        """
        ___init___

        Initialise class members
        """
        BaseWorkerThread.__init__(self)

        self.enabled = config.RucioInjector.enabled
        # dataset rule creation has a larger polling cycle
        self.pollRules = config.RucioInjector.pollIntervalRules
        self.lastRulesExecTime = 0
        self.createBlockRules = config.RucioInjector.createBlockRules
        self.skipRulesForTiers = config.RucioInjector.skipRulesForTiers
        self.listTiersToInject = config.RucioInjector.listTiersToInject

        # setup cache for container and blocks (containers can be much longer, make 6 days now)
        self.containersCache = MemoryCache(config.RucioInjector.cacheExpiration * 3, set())
        self.blocksCache = MemoryCache(config.RucioInjector.cacheExpiration, set())

        self.scope = getattr(config.RucioInjector, "scope", "cms")
        self.rucioAcct = config.RucioInjector.rucioAccount
        self.rucio = Rucio(acct=self.rucioAcct,
                           hostUrl=config.RucioInjector.rucioUrl,
                           authUrl=config.RucioInjector.rucioAuthUrl,
                           configDict={'logger': self.logger})

        # metadata dictionary information to be added to block/container rules
        # cannot be a python dictionary, but a JSON string instead
        self.metaData = json.dumps(dict(agentHost=config.Agent.hostName,
                                        userAgent=config.Agent.agentName))

        self.testRSEs = config.RucioInjector.RSEPostfix
        self.filesToRecover = []

        logging.info("Component configured to skip container rule creation for data tiers: %s",
                     self.skipRulesForTiers)
        logging.info("Component configured to create block rules: %s", self.createBlockRules)

    def setup(self, parameters):
        """
        _setup_

        Create DAO Factory and setup some DAO.
        """
        myThread = threading.currentThread()
        daofactory = DAOFactory(package="WMComponent.RucioInjector.Database",
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

    @timeFunction
    def algorithm(self, parameters):
        """
        _algorithm_

        Poll the database for uninjected files and inject them into Rucio.
        """
        if not self.enabled:
            logging.info("RucioInjector component is disabled in the configuration, exiting.")
            return

        logging.info("Running Rucio injector poller algorithm...")

        try:
            # files that failed to get their status updated in dbsbuffer
            self._updateLFNState(self.filesToRecover, recovery=True)

            # get dbsbuffer_file.in_phedex = 0
            uninjectedFiles = self.getUninjected.execute()

            # while we commission Rucio within WM, not all datatiers are supposed
            # to be injected by this component. Remove any data that we are not
            # meant to process!
            uninjectedFiles = filterDataByTier(uninjectedFiles, self.listTiersToInject)

            # create containers in rucio  (and update local cache)
            containersAdded = self.insertContainers(uninjectedFiles)
            if self.containersCache.isCacheExpired():
                self.containersCache.setCache(containersAdded)
            else:
                self.containersCache.addItemToCache(containersAdded)

            # create blocks. Only update the cache once a rule gets created...
            blocksAdded = self.insertBlocks(uninjectedFiles)

            # create file replicas
            self.insertReplicas(uninjectedFiles)

            # now create a Rucio rule for every single block (and update local cache)
            blocksAdded = self.insertBlockRules(blocksAdded)
            if self.blocksCache.isCacheExpired():
                self.blocksCache.setCache(blocksAdded)
            else:
                self.blocksCache.addItemToCache(blocksAdded)

            # now close blocks already uploaded to DBS
            self.closeBlocks()

            if self.lastRulesExecTime + self.pollRules <= int(time.time()):
                self.deleteBlocks()
                self.insertContainerRules()
        except Exception as ex:
            msg = "Caught unexpected exception in RucioInjector. Details:\n%s" % str(ex)
            logging.exception(msg)
            raise RucioInjectorException(msg)

        return

    def insertContainers(self, uninjectedData):
        """
        This method will insert containers into Rucio, provided they cannot be found in
        the local cache.
        :param uninjectedData: same data as it's returned from the uninjectedFiles
        :return: set of containers successfully inserted into Rucio
        """
        logging.info("Preparing to insert containers into Rucio...")
        newContainers = set()
        for location in uninjectedData:
            for container in uninjectedData[location]:
                if container not in self.containersCache:
                    if self.rucio.createContainer(container):
                        logging.info("Container %s inserted into Rucio", container)
                        newContainers.add(container)
                    else:
                        logging.error("Failed to create container: %s", container)
        logging.info("Successfully inserted %d containers into Rucio", newContainers)
        return newContainers

    def insertBlocks(self, uninjectedData):
        """
        This method will insert blocks into Rucio and attach them to their correspondent
        containers, when attaching this block, we also need to provide the RSE that it
        will be available.
        :param uninjectedData: same data as it's returned from the uninjectedFiles
        :return: a dictionary of successfully inserted blocks and their correspondent location
        """
        logging.info("Preparing to insert blocks into Rucio...")
        newBlocks = {}
        for location in uninjectedData:
            if self.testRSEs:
                rseName = "%s_Test" % location
            for container in uninjectedData[location]:
                for block in uninjectedData[location][container]:
                    if block not in self.blocksCache:
                        if self.rucio.createBlock(block, rse=rseName):
                            logging.info("Block %s inserted into Rucio", block)
                            newBlocks[block] = location
                        else:
                            logging.error("Failed to create block: %s", block)
        logging.info("Successfully inserted %d blocks into Rucio", newBlocks)
        return newBlocks

    # TODO: this will likely go away once the phedex to rucio migration is over
    def _isBlockTierAllowed(self, blockName):
        """
        Check the block datatier against the list of data tiers that we want
        to get pinned in Rucio through a block rule
        :return: boolean whether we can or not insert a rule
        """
        endBlock = blockName.rsplit('/', 1)[1]
        endTier = endBlock.split('#')[0]
        if endTier in self.skipRulesForTiers:
            return False
        return True

    def insertBlockRules(self, blocksAndRSEs):
        """
        Creates a simple replication rule for every single block that
        has been previously inserted into Rucio.
        Right now, we make standard rules against a given site.
        :param blocksAndRSEs: dictionary with blocks as key, and RSEs as value
        :return: a set of blocks with successfully created rules
        """
        newBlocks = set()
        if not self.createBlockRules:
            return newBlocks

        # FIXME: this could have been done in bulk, by RSE
        logging.info("Preparing to create block rules into Rucio...")
        for block, rse in blocksAndRSEs.items():
            if not self._isBlockTierAllowed(block):
                logging.info("Component configured to skip block rule for: %s", block)
                continue
            if self.testRSEs:
                rseName = "%s_Test" % rse
            # DATASET = replicates all files in the same block to the same RSE
            resp = self.rucio.createReplicationRule(block, rseExpression="rse=%s" % rseName,
                                                    account=self.rucioAcct, grouping="DATASET",
                                                    comment="WMAgent production site",
                                                    meta=self.metaData)
            if resp:
                logging.info("Block rule created for %s under rule id: %s", block, resp)
                newBlocks.add(block)
            else:
                logging.error("Failed to create rule for block: %s", block)
        logging.info("Successfully inserted %d rules for blocks into Rucio", newBlocks)
        return newBlocks

    def insertReplicas(self, uninjectedData):
        """
        Inserts replicas into Rucio and attach them to its specific block.
        If the insertion succeeds, also switch their database state to injected.

        :param uninjectedData: dictionary with blocks as key, and RSEs as value
        """
        # FIXME: I think we need a different data struct from the database
        # this method is very expensive O(n^4)
        logging.info("Preparing to insert replicas into Rucio...")

        for location in uninjectedData.keys():
            if self.testRSEs:
                rseName = "%s_Test" % location
            for container in uninjectedData[location]:
                for block in uninjectedData[location][container]:
                    injectData = []
                    listLfns = []
                    for fileInfo in uninjectedData[location][container][block]['files']:
                        listLfns.append(fileInfo['lfn'])
                        injectData.append(dict(name=fileInfo['lfn'], scope=self.scope,
                                               bytes=fileInfo['size'], state="A",
                                               adler32=fileInfo['checksum']['adler32']))

                    if self.rucio.createReplicas(rse=rseName, files=injectData, block=block):
                        logging.info("Successfully inserted %d files on block %s", len(listLfns), block)
                        self._updateLFNState(listLfns)
        return

    def _updateLFNState(self, listLfns, recovery=False):
        """
        Given a list of LFNs, update their state in dbsbuffer table.
        :param listLfns: list of LFNs
        :param recovery: True if we are recovering previously injected files
        :return: nothing
        """
        if not listLfns:
            return
        try:
            self.setStatus.execute(listLfns, 1)
        except Exception as ex:
            # save it to try to inject them again in the next cycle
            self.filesToRecover.extend(listLfns)
            if 'Deadlock found' in str(ex) or 'deadlock detected' in str(ex):
                logging.error("Deadlock during file status update. Retrying again in the next cycle.")
                self.filesToRecover.extend(listLfns)
            else:
                msg = "Failed to update file status in the database, reason: %s" % str(ex)
                logging.error(msg)
                raise RucioInjectorException(msg)
        else:
            if recovery:
                self.filesToRecover = []

    def closeBlocks(self):
        """
        Close any blocks that have been migrated to global DBS
        """
        logging.info("Starting closeBlocks method")

        # in short, dbsbuffer_file.in_phedex = 1 AND dbsbuffer_block.status = 'InDBS'
        migratedBlocks = self.getMigrated.execute()
        ### FIXME the data format returned by this DAO
        for location in migratedBlocks:
            for container in migratedBlocks[location]:
                for block in migratedBlocks[location][container]:
                    if self.rucio.closeBlockContainer(block):
                        self.setBlockClosed.execute(block)
                    else:
                        logging.error("Failed to close block: %s. Will retry again later.", block)

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
        # FIXME: figure out the proper logic for rule block deletion
        logging.info("Starting deleteBlocks methods --> IMPLEMENT-ME!!!")

    # TODO: this will likely go away once the phedex to rucio migration is over
    def _isContainerTierAllowed(self, containerName):
        """
        Check the container datatier against the list of data tiers that we want
        to get pinned in Rucio through a container rule
        :return: boolean whether we can or not insert a rule
        """
        endTier = containerName.rsplit('/', 1)[1]
        if endTier in self.skipRulesForTiers:
            return False
        return True

    def insertContainerRules(self):
        """
        _insertContainerRules_
        Poll the database for datasets meant to be subscribed and create
        a container level rule to replicate all files to a given RSE
        """
        logging.info("Starting insertContainerRules method")

        # FIXME also adapt the format returned by this DAO
        # Check for completely unsubscribed datasets
        # in short, files in phedex, file status in "GLOBAL" or "InDBS", and subscribed=0
        unsubscribedDatasets = self.getUnsubscribed.execute()

        # Keep a list of subscriptions to tick as subscribed in the database
        subscriptionsMade = []

        # Create the subscription objects and add them to the list
        # The list takes care of the sorting internally
        for subInfo in unsubscribedDatasets:
            rse = subInfo['site']
            container = subInfo['path']
            if not self._isContainerTierAllowed(container):
                logging.info("Component configured to skip container rule for: %s", container)
                continue
            logging.info("Creating container rule for %s against RSE %s", container, rse)

            if self.testRSEs:
                rseName = "%s_Test" % rse
            # ALL = replicates all files to the same RSE
            resp = self.rucio.createReplicationRule(container, rseExpression="rse=%s" % rseName,
                                                    account=self.rucioAcct, grouping="ALL",
                                                    comment="WMCore automatic container rule",
                                                    meta=self.metaData)
            if resp:
                logging.info("Container rule created for %s under rule id: %s", container, resp)
                subscriptionsMade.append(subInfo['id'])
            else:
                logging.error("Failed to create rule for block: %s", container)

        # Register the result in DBSBuffer
        if subscriptionsMade:
            self.markSubscribed.execute(subscriptionsMade)

        return
