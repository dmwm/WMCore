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

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.Agent.Harness import Harness
from WMCore.DAOFactory import DAOFactory
from WMCore.Database.DBFactory import DBFactory

from T0.State.Database.Reader import ListBlock
from T0.State.Database.Reader import ListDatasets
from T0.State.Database.Reader import ListFiles
from T0.State.Database.Reader import ListRuns

from T0.State.Database.Writer import InsertBlock
from T0.State.Database.Writer import InsertDataset

from T0.GenericTier0.Tier0DB import Tier0DB
from T0.RunConfigCache.CacheManager import getRunConfigCache

from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset

from WMCore.DataStructs.Run import Run
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Services.UUID import makeUUID

from WMCore.WMSpec.StdSpecs.PromptSkim import PromptSkimWorkloadFactory
from WMCore.WMSpec.Makers.TaskMaker import TaskMaker
from WMCore.WorkQueue.WMBSHelper import WMBSHelper

from WMComponent.DBSBuffer.Database.Interface.DBSBufferFile import DBSBufferFile

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
        os.makedirs(self.workloadCache)

        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package = "WMCore.WMBS", logger = logging,
                                     dbinterface = myThread.dbi)
        self.runConfigCache = None

        # Scram arch and path to cmssw needed to generate workflows.
        self.scramArch = self.config.PromptSkimScheduler.scramArch
        self.cmsPath = self.config.PromptSkimScheduler.cmsPath

        # Job splitting parameters
        self.filesPerJob = self.config.PromptSkimScheduler.filesPerJob
        self.minMergeSize = self.config.PromptSkimScheduler.minMergeSize
        self.maxMergeEvents = self.config.PromptSkimScheduler.maxMergeEvents
        self.maxMergeSize = self.config.PromptSkimScheduler.maxMergeSize
        self.maxMergeFiles = self.config.PromptSkimScheduler.maxMergeFiles

        phedex = PhEDEx({"endpoint": self.config.PromptSkimScheduler.phedexURL}, "json")
        self.nodeMap = phedex.getNodeMap()
        
        self.t0astDBConn = None
        self.connectT0AST()
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
        self.pollForRunComplete()
        return

    def getRunConfig(self, runNumber):
        """
        _getRunConfig_

        Get a RunConfig instance for the given run number.
        """
        if not self.runConfigCache:
            self.runConfigCache = getRunConfigCache(self.t0astDBConn, None)
            self.runConfigCache.configCache = os.path.join(self.config.PromptSkimScheduler.workloadCache,
                                                           "RunConfig")

        return self.runConfigCache.getRunConfig(runNumber)
    
    def inputFilesetName(self, blockInfo):
        """
        _inputFilesetName_

        Generate a name for the fileset that will be used to hold files from
        a freshly transfered block.  The name will take the following form:
          T0-RunNNN-PrimaryDatasetName-ProcessedDatasetName-DataTierName
        """
        filesetName = "T0-Run%s-%s-%s-%s" % (blockInfo["RUN_ID"],
                                             blockInfo.getPrimaryDatasetName(),
                                             blockInfo.getProcessedDatasetName(),
                                             blockInfo.getDataTier())
        return filesetName

    def insertFileParentsIntoTier1WMBS(self, childFile):
        """
        _insertFileParentsIntoTier1WMBS_

        For a given file, insert it's parents into the Tier1 WMBS.  This is used
        for workflows that do two file reads.  We don't need to bother inserting
        any meta data because we only need the parent LFN to create the
        jobspec.
        """        
        parentLFNs = ListFiles.listParentLFNs(self.t0astDBConn, childFile["lfn"])

        for parentLFN in parentLFNs:
            parentFile = File(lfn = parentLFN)
            parentFile.create()

            childFile.addParent(parentLFN)

        return

    def insertFilesIntoTier1WMBS(self, blockInfo, blockLocation,
                                 insertParents = False):
        """
        _insertFilesIntoTier1WMBS_

        Insert all the files from the given block into the Tier1 WMBS.  Insert
        the newly added files with the given fileset.  Also add run and lumi
        information for each file that was added.
        """
        inputFilesetName = self.inputFilesetName(blockInfo)

        locationNew = self.daoFactory(classname = "Locations.New")
        locationNew.execute(siteName = blockLocation, seName = blockLocation)

        newFileset = Fileset(name = inputFilesetName, is_open = True)
        newFileset.create()

        blockFiles = ListFiles.listBlockFilesForSkim(self.t0astDBConn,
                                                     blockInfo["BLOCK_ID"])

        for file in blockFiles:
            dbsFile = DBSBufferFile(lfn = file["LFN"], status = "AlreadyInDBS")
            dbsFile.setDatasetPath("bogus")
            dbsFile.setAlgorithm(appName = "cmsRun", appVer = "UNKNOWN",
                                 appFam = "UNKNOWN", psetHash = "GIBBERISH",
                                 configContent = "GIBBERISH")
            dbsFile.create()
            
            newFile = File(lfn = file["LFN"], size = file["SIZE"],
                           events = file["EVENTS"])

            lumiNumbers = ListFiles.listLumiInfoForFile(self.t0astDBConn,
                                                        file["LFN"])
            runInfo = Run(runNumber = blockInfo["RUN_ID"])
            runInfo.extend(lumiNumbers)

            newFile.addRun(runInfo)
            newFile.setLocation(blockLocation, immediateSave = False)
            newFile.create()
            if insertParents:
                self.insertFileParentsIntoTier1WMBS(newFile)
            
            newFileset.addFile(newFile)

        newFileset.commit()
        return

    def createWorkloadsForBlock(self, acquisitionEra, skimConfig, blockInfo,
                                blockLocation):
        """
        _createWorkloadsForBlock_

        Check to see if we're already created skimming workloads for the
        run/dataset that the block belongs to.  If no workload exists create one
        and install it into WMBS.
        """
        if self.workloads.has_key(blockInfo["RUN_ID"]):
            if self.workloads[blockInfo["RUN_ID"]].has_key(skimConfig.SkimName):
                return

        runConfig = self.getRunConfig(blockInfo["RUN_ID"])
        (datasetPath, guid) = blockInfo["BLOCK_NAME"].split("#", 1)
        configFile = runConfig.retrieveConfigFromURL(skimConfig.ConfigURL)

        if skimConfig.TwoFileRead:
            splitAlgo = "TwoFileBased"
        else:
            splitAlgo = "FileBased"
            
        wfParams = {"AcquisitionEra": runConfig.getAcquisitionEra(),
                    "Requestor": "CMSPromptSkimming",
                    "InputDataset": datasetPath,
                    "CMSSWVersion": skimConfig.CMSSWVersion,
                    "ScramArch": self.scramArch,
                    "ProcessingVersion": skimConfig.ProcessingVersion,
                    "GlobalTag": skimConfig.GlobalTag,
                    "CmsPath": self.cmsPath,
                    "SkimConfig": configFile,
                    "UnmergedLFNBase": "/store/unmerged",
                    "MergedLFNBase": "/store",
                    "MinMergeSize": self.minMergeSize,
                    "MaxMergeSize": self.maxMergeSize,
                    "MaxMergeEvents": self.maxMergeEvents,
                    "SplitAlgo": splitAlgo}

        (primary, processed, tier) = datasetPath[1:].split("/", 3)
        workloadName = "Run%s-%s-%s-%s" % (blockInfo["RUN_ID"], primary, processed, skimConfig.SkimName)

        workload = self.promptSkimFactory(workloadName, wfParams)
        taskMaker = TaskMaker(workload, os.path.join(self.workloadCache, workloadName))
        taskMaker.skipSubscription = True
        taskMaker.processWorkload()

        specPath = os.path.join(self.workloadCache, workloadName, "spec.pkl")
        workload.save(specPath)

        myHelper = WMBSHelper(workload, specPath, "CMSDataOps", "PromptSkim",
                              "Skim", None, None, None)
        myHelper.createSubscription(self.inputFilesetName(blockInfo))

        if not self.workloads.has_key(blockInfo["RUN_ID"]):
            self.workloads[blockInfo["RUN_ID"]] = {}
        self.workloads[blockInfo["RUN_ID"]][skimConfig.SkimName] = True
        return

    def pollForTransferedBlocks(self):
        """
        _pollForTransferedBlocks_

        Poll T0AST for any blocks that have been migrated to DBS and generate
        skims for them.  Mark the blocks as "Skimmed" once any skims have been
        injected into the Tier1 WMBS.
        """
        logging.debug("pollForTransferedBlocks(): Running...")
        
        skimmableBlocks = ListBlock.listBlockInfoByStatus(self.t0astDBConn,
                                                          "Exported", "Migrated")

        logging.debug("pollForTransferedBlocks(): Found %s blocks." % len(skimmableBlocks))

        for skimmableBlock in skimmableBlocks:
            logging.debug("pollForTransferedBlocks(): Skimmable: %s" % skimmableBlock["BLOCK_ID"])
            runConfig = self.getRunConfig(int(skimmableBlock["RUN_ID"]))
            
            skims = runConfig.getSkimConfiguration(skimmableBlock["PRIMARY_ID"],
                                                   skimmableBlock["TIER_ID"])

            if skims == None:
                InsertBlock.updateBlockStatusByID(self.t0astDBConn,
                                                  skimmableBlock, "Skimmed")
                self.t0astDBConn.commit()
                continue

            insertParents = False
            for skimConfig in skims:
                if skimConfig.TwoFileRead:
                    insertParents = True
                    break

            if insertParents:
                if not ListBlock.isParentBlockExported(self.t0astDBConn, skimmableBlock["BLOCK_ID"]):
                    logging.debug("Block %s has unexported parents." % skimmableBlock["BLOCK_ID"])
                    continue

            blockLocation = skimmableBlock["STORAGE_NODE"]
            if skimmableBlock["CUSTODIAL"] != 1:
                continue

            myThread = threading.currentThread()
            myThread.transaction.begin()

            for phedexNode in self.nodeMap["phedex"]["node"]:
                if phedexNode["name"] == blockLocation:
                    blockSEName = str(phedexNode["se"])

            self.insertFilesIntoTier1WMBS(skimmableBlock, blockSEName,
                                          insertParents)

            for skimConfig in skims:
                try:
                    self.createWorkloadsForBlock(runConfig.getAcquisitionEra(),
                                                 skimConfig, skimmableBlock,
                                                 blockSEName)
                except Exception, ex:
                    logging.debug("Error making workflows: %s" % str(ex))
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

    def pollForRunComplete(self):
        """
        _pollForRunComplete_

        Query the Tier1 WMBS for any open filesets that start with "T0".  These
        filesets contain files that are injected from the Tier0 into the Tier1
        processing system.  Given the list of open T0 filesets determine what
        run they belong to and mark them as closed if the T0 is done processing
        data for that run.
        """
        myThread = threading.currentThread()
        myThread.transaction.begin()

        openFilesetDAO = self.daoFactory(classname = "Fileset.ListOpen")
        openFilesetNames = openFilesetDAO.execute()

        openInputFilesets = []
        openRuns = []
        for openFilesetName in openFilesetNames:
            (type, run, dataset) = openFilesetName.split("-", 2)
            if type == "T0":
                openInputFilesets.append(openFilesetName)
                if run[3:] not in openRuns:
                    openRuns.append(run[3:])

        if len(openRuns) == 0:
            myThread.transaction.commit()
            return
        
        runStatuses = ListRuns.listRunState(self.t0astDBConn, openRuns)
        blockCount = ListBlock.countUnExportedBlocksByRun(self.t0astDBConn,
                                                         openRuns)

        for openInputFileset in openInputFilesets:
            (type, run, dataset) = openInputFileset.split("-", 2)
            datasetParts = dataset.split("-")
            primary = datasetParts[0]
            tier = datasetParts[-1]
            
            runNumber = int(run[3:])
            runStatus = runStatuses[runNumber]

            if runStatus == "CloseOutExport":
                logging.debug("Checking %s for closeout..." % openInputFileset)
                if blockCount.has_key(runNumber):
                    if blockCount[runNumber].has_key(primary):
                        if blockCount[runNumber][primary].has_key(tier):
                            logging.debug("  %s has %s open blocks." % (openInputFileset,
                                                                        blockCount[runNumber][primary][tier]))
                            continue
            elif runStatus != "Complete":
                    continue

            logging.debug("Closing fileset: %s" % openInputFileset)
            wmbsFileset = Fileset(name = openInputFileset)
            wmbsFileset.markOpen(False)

        myThread.transaction.commit()
        return
