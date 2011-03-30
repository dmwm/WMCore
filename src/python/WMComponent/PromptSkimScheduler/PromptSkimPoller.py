#!/usr/bin/env python
"""
_PromptSkimPoller_

Poll T0AST for complete blocks and launch skims.
"""

import time
import threading
import logging
import sys
import os
import traceback

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.Agent.Harness import Harness
from WMCore.DAOFactory import DAOFactory
from WMCore.Database.DBFactory import DBFactory

from T0.State.Database.Reader import ListBlock
from T0.State.Database.Writer import InsertBlock

from T0.GenericTier0.Tier0DB import Tier0DB
from T0.RunConfigCache.Cache import Cache

from WMCore.WMSpec.StdSpecs.PromptSkim import PromptSkimWorkloadFactory
from WMCore.WorkQueue.WorkQueue import WorkQueue

class PromptSkimPoller(BaseWorkerThread):
    def __init__(self, config):
        BaseWorkerThread.__init__(self)
        self.config = config
        return
    
    def setup(self, parameters = None):
        """
        _setup_

        Setup all the parameters.
        """
        logging.info("PromptSkimScheduler Component Started")

        # Workload related parameters
        self.promptSkimFactory = PromptSkimWorkloadFactory()
        self.workloads = {}
        self.workloadCache = self.config.PromptSkimScheduler.workloadCache

        if not os.path.exists(self.workloadCache):
            os.makedirs(self.workloadCache)

        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package = "WMCore.WMBS", logger = logging,
                                     dbinterface = myThread.dbi)
        self.runConfigCache = None

        # Scram arch and path to cmssw needed to generate workflows.
        self.scramArch = self.config.PromptSkimScheduler.scramArch
        self.cmsPath = self.config.PromptSkimScheduler.cmsPath
        self.initCommand = self.config.PromptSkimScheduler.initCommand

        # Job splitting parameters
        self.minMergeSize = self.config.PromptSkimScheduler.minMergeSize
        self.maxMergeEvents = self.config.PromptSkimScheduler.maxMergeEvents
        self.maxMergeSize = self.config.PromptSkimScheduler.maxMergeSize
        self.maxMergeFiles = self.config.PromptSkimScheduler.maxMergeFiles

        self.t0astDBConn = None
        self.connectT0AST()

        self.workQueue = WorkQueue()
        return

    def connectT0AST(self):
        """
        _connectT0AST_

        Create a T0AST DB connection object.
        """
        self.t0astDBConn = Tier0DB(connectionParameters = {},
                                   manageGlobal = False,
                                   initConnection = False)

        self.t0astDBConn.dbFactory = DBFactory(logging, self.config.PromptSkimScheduler.t0astURL)
        self.t0astDBConn.connect()
        return

    def algorithm(self, parameters = None):
        """
        _algorithm_

        Poll for transfered blocks and complete runs.
        """
        self.pollForTransferedBlocks()
        return

    def getRunConfig(self, runNumber):
        """
        _getRunConfig_

        Get a RunConfig instance for the given run number.
        """
        if not self.runConfigCache:
            self.runConfigCache = Cache(promptSkimming = True)
            self.runConfigCache.t0astDBConn = self.t0astDBConn
            self.runConfigCache.configCache = os.path.join(self.config.PromptSkimScheduler.workloadCache,
                                                           "RunConfig")

        return self.runConfigCache.getRunConfig(runNumber)
    
    def createWorkloadsForBlock(self, acquisitionEra, skimConfig, blockInfo):
        """
        _createWorkloadsForBlock_

        Check to see if we're already created skimming workloads for the
        run/dataset that the block belongs to.  If no workload exists create one
        and install it into WMBS.
        """
        (datasetPath, guid) = blockInfo["BLOCK_NAME"].split("#", 1)
        (primary, processed, tier) = datasetPath[1:].split("/", 3)
        workloadName = "Run%s-%s-%s-%s" % (blockInfo["RUN_ID"], primary, processed, skimConfig.SkimName)

        if self.workloads.has_key(blockInfo["RUN_ID"]):
            if self.workloads[blockInfo["RUN_ID"]].has_key(skimConfig.SkimName):
                workload = self.workloads[blockInfo["RUN_ID"]][skimConfig.SkimName]
                workload.setBlockWhitelist(blockInfo["BLOCK_NAME"])
                specPath = os.path.join(self.workloadCache, workloadName, "%s.pkl" % guid)
                workload.setSpecUrl(specPath)
                workload.save(specPath)  
                self.workQueue.queueWork(specPath, team = "PromptSkimming", request = workloadName)                
                return

        runConfig = self.getRunConfig(blockInfo["RUN_ID"])
        configFile = runConfig.retrieveConfigFromURL(skimConfig.ConfigURL)

        if skimConfig.TwoFileRead:
            splitAlgo = "TwoFileBased"
        else:
            splitAlgo = "FileBased"

        blockLocation = blockInfo["STORAGE_NODE"].replace("_MSS", "")

        wfParams = {"AcquisitionEra": runConfig.getAcquisitionEra(),
                    "Requestor": "CMSPromptSkimming",
                    "CustodialSite": blockLocation,
                    "BlockName": blockInfo["BLOCK_NAME"],
                    "InputDataset": datasetPath,
                    "CMSSWVersion": skimConfig.CMSSWVersion,
                    "ScramArch": self.scramArch,
                    "InitCommand": self.initCommand,
                    "CouchURL": self.config.JobStateMachine.couchurl,
                    "CouchDBName": self.config.JobStateMachine.configCacheDBName,
                    "ProcessingVersion": skimConfig.ProcessingVersion,
                    "GlobalTag": skimConfig.GlobalTag,
                    "CmsPath": self.cmsPath,
                    "SkimConfig": configFile,
                    "UnmergedLFNBase": "/store/unmerged",
                    "MergedLFNBase": "/store/data",
                    "MinMergeSize": self.minMergeSize,
                    "MaxMergeSize": self.maxMergeSize,
                    "MaxMergeEvents": self.maxMergeEvents,
                    "StdJobSplitAlgo": splitAlgo,
                    "StdJobSplitArgs": {"files_per_job": 1},
                    "ValidStatus": "VALID"}

        workload = self.promptSkimFactory(workloadName, wfParams)
        workload.setOwner("CMSDataOps")

        if not os.path.exists(os.path.join(self.workloadCache, workloadName)):
            os.makedirs(os.path.join(self.workloadCache, workloadName))
            
        specPath = os.path.join(self.workloadCache, workloadName, "%s.pkl" % guid)        
        workload.setSpecUrl(specPath)
        workload.save(specPath)

        self.workQueue.queueWork(specPath, team = "PromptSkimming", request = workloadName)

        if not self.workloads.has_key(blockInfo["RUN_ID"]):
            self.workloads[blockInfo["RUN_ID"]] = {}
        self.workloads[blockInfo["RUN_ID"]][skimConfig.SkimName] = workload
        return

    def pollForTransferedBlocks(self):
        """
        _pollForTransferedBlocks_

        Poll T0AST for any blocks that have been migrated to DBS and generate
        skims for them.  Mark the blocks as "Skimmed" once any skims have been
        injected into the Tier1 WMBS.
        """
        logging.info("pollForTransferedBlocks(): Running...")
        
        skimmableBlocks = ListBlock.listBlockInfoByStatus(self.t0astDBConn,
                                                          "Exported", "Migrated")

        logging.info("pollForTransferedBlocks(): Found %s blocks." % len(skimmableBlocks))

        for skimmableBlock in skimmableBlocks:
            logging.info("pollForTransferedBlocks(): Skimmable: %s" % skimmableBlock["BLOCK_ID"])
            runConfig = self.getRunConfig(int(skimmableBlock["RUN_ID"]))
            
            skims = runConfig.getSkimConfiguration(skimmableBlock["PRIMARY_ID"],
                                                   skimmableBlock["TIER_ID"])

            if skims == None:
                InsertBlock.updateBlockStatusByID(self.t0astDBConn,
                                                  skimmableBlock, "Skimmed")
                self.t0astDBConn.commit()
                logging.info("No skims for block %s" % skimmableBlock["BLOCK_ID"])
                continue

            insertParents = False
            for skimConfig in skims:
                if skimConfig.TwoFileRead:
                    insertParents = True
                    break

            if insertParents:
                if not ListBlock.isParentBlockExported(self.t0astDBConn, skimmableBlock["BLOCK_ID"]):
                    logging.info("Block %s has unexported parents." % skimmableBlock["BLOCK_ID"])
                    continue

            blockLocation = skimmableBlock["STORAGE_NODE"]
            if skimmableBlock["CUSTODIAL"] != 1:
                logging.info("Skipping block %s, this isn't it's custodial site." % skimmableBlock["BLOCK_ID"])
                continue

            myThread = threading.currentThread()
            myThread.transaction.begin()

            for skimConfig in skims:
                try:
                    self.createWorkloadsForBlock(runConfig.getAcquisitionEra(),
                                                 skimConfig, skimmableBlock)
                except Exception, ex:
                    logging.info("Error making workflows: %s" % str(ex))
                    logging.info("Traceback: %s" % traceback.format_exc())
                    self.t0astDBConn.rollback()
                    myThread.transaction.rollback()
                    break
            else:
                InsertBlock.updateBlockStatusByID(self.t0astDBConn, skimmableBlock,
                                                  "Skimmed")
                self.t0astDBConn.commit()
                myThread.transaction.commit()

        self.t0astDBConn.commit()
        return
