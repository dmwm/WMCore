#!/usr/bin/env python

import os
import sys
import imp
import pickle

from WMCore.Configuration import loadConfigurationFile
from WMCore.Cache.ConfigCache import WMConfigCache
from WMCore.Services.UUID import makeUUID
from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.WMSpec.Makers.TaskMaker import TaskMaker
from WMCore.WMInit import WMInit

from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.DataStructs.Run import Run
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.Subscription import Subscription

from DBSAPI.dbsApi import DbsApi

cmsPath = "/uscmst1/prod/sw/cms"
scramArch = "slc5_ia32_gcc434"
processingConfig = "/uscms/home/sfoulkes/rereco_FirstCollisions_MinimumBias_35X.py"
frameworkVersion = "CMSSW_3_5_8_patch3"
mergedLFNBase = "/store/temp/WMAgent/merged/"
unmergedLFNBase = "/store/temp/WMAgent/unmerged/"
acquisitionEra = "WMAgentCommissioning10"
processingVersion = "v1scf"
primaryDatasetName = "MinimumBias"
minMergeSize = 500000000
maxMergeSize = 4294967296
workloadName = "ReReco"
inputDatasetName = "/MinimumBias/Commissioning10-v4/RAW"

def createWorkload(workloadName):
    """
    _createWorkload_

    """
    workload = newWorkload(workloadName)
    workload.setOwner("sfoulkes@fnal.gov")
    workload.setStartPolicy('DatasetBlock')
    workload.setEndPolicy('SingleShot')
    return workload

def addProcessingTask(workload, taskName, inputDataset, couchUrl, couchDBName, configDoc):
    """
    _addProcessingTask_

    """
    procTask = workload.newTask(taskName)
    procTaskCmssw = procTask.makeStep("cmsRun1")
    procTaskCmssw.setStepType("CMSSW")
    procTaskStageOut = procTaskCmssw.addStep("stageOut1")
    procTaskStageOut.setStepType("StageOut")
    procTaskLogArch = procTaskCmssw.addStep("logArch1")
    procTaskLogArch.setStepType("LogArchive")
    procTask.applyTemplates()
    procTask.setSplittingAlgorithm("FileBased", files_per_job = 1)
    procTask.addGenerator("BasicNaming")
    procTask.addGenerator("BasicCounter")
    procTask.setTaskType("Processing")

    (primary, processed, tier) = inputDataset[1:].split("/")
    procTask.addInputDataset(primary = primary, processed = processed, tier = tier)

    procTaskCmsswHelper = procTaskCmssw.getTypeHelper()
    procTaskCmsswHelper.cmsswSetup(frameworkVersion, softwareEnvironment = "",
                                   scramArch = scramArch)
    procTaskCmsswHelper.setConfigCache(couchUrl, configDoc, couchDBName)
    return procTask

def addLogCollectTask(parentTask):
    """
    _addLogCollecTask_
    
    Create a LogCollect task for log archives that are produced by the
    parent task.
    """
    logCollectTask = parentTask.addTask("LogCollect")
    logCollectStep = logCollectTask.makeStep("logCollect1")
    logCollectStep.setStepType("LogCollect")
    logCollectTask.applyTemplates()
    logCollectTask.setSplittingAlgorithm("EndOfRun", files_per_job = 500)
    logCollectTask.addGenerator("BasicNaming")
    logCollectTask.addGenerator("BasicCounter")
    logCollectTask.setTaskType("LogCollect")
    
    parentTaskLogArch = parentTask.getStep("logArch1")
    logCollectTask.setInputReference(parentTaskLogArch, outputModule = "logArchive")
    return

def addOutputModule(parentTask, outputModuleName, dataTier, filterName):
    """
    _addOutputModule_
    
    Add an output module and merge task for files produced by the parent
    task.
    """
    if filterName != None:
        processedDatasetName = "%s-%s-%s" % (acquisitionEra, filterName, processingVersion)
    else:
        processedDatasetName = "%s-%s" % (acquisitionEra, processingVersion)
        
    unmergedLFN = "%s/%s/%s" % (unmergedLFNBase, dataTier, processedDatasetName)
    mergedLFN = "%s/%s/%s" % (mergedLFNBase, dataTier, processedDatasetName)
    cmsswStep = parentTask.getStep("cmsRun1")
    cmsswStepHelper = cmsswStep.getTypeHelper()
    cmsswStepHelper.addOutputModule(outputModuleName,
                                    primaryDataset = primaryDatasetName,
                                    processedDataset = processedDatasetName,
                                    dataTier = dataTier,
                                    lfnBase = unmergedLFN,
                                    mergedLFNBase = mergedLFN)
    addMergeTask(parentTask, outputModuleName, dataTier, processedDatasetName)
    return

def addMergeTask(parentTask, parentOutputModule, dataTier, processedDatasetName):
    """
    _addMergeTask_
    
    Create a merge task for files produced by the parent task.
    """
    mergeTask = parentTask.addTask("Merge%s" % parentOutputModule.capitalize())
    mergeTaskCmssw = mergeTask.makeStep("cmsRun1")
    mergeTaskCmssw.setStepType("CMSSW")
        
    mergeTaskStageOut = mergeTaskCmssw.addStep("stageOut1")
    mergeTaskStageOut.setStepType("StageOut")
    mergeTaskLogArch = mergeTaskCmssw.addStep("logArch1")
    mergeTaskLogArch.setStepType("LogArchive")
    mergeTask.addGenerator("BasicNaming")
    mergeTask.addGenerator("BasicCounter")
    mergeTask.setTaskType("Merge")  
    mergeTask.applyTemplates()
    mergeTask.setSplittingAlgorithm("WMBSMergeBySize",
                                    max_merge_size = maxMergeSize,
                                    min_merge_size = minMergeSize)
    
    mergeTaskCmsswHelper = mergeTaskCmssw.getTypeHelper()
    mergeTaskCmsswHelper.cmsswSetup(frameworkVersion, softwareEnvironment = "",
                                    scramArch = scramArch)
    mergeTaskCmsswHelper.setDataProcessingConfig("cosmics", "merge")

    mergedLFN = "%s/%s/%s" % (mergedLFNBase, dataTier, processedDatasetName)    
    mergeTaskCmsswHelper.addOutputModule("Merged",
                                         primaryDataset = primaryDatasetName,
                                         processedDataset = processedDatasetName,
                                         dataTier = dataTier,
                                         lfnBase = mergedLFN)
    
    parentTaskCmssw = parentTask.getStep("cmsRun1")
    mergeTask.setInputReference(parentTaskCmssw, outputModule = parentOutputModule)
    addCleanupTask(mergeTask, parentOutputModule)
    return

def addCleanupTask(parentTask, parentOutputModuleName):
    """
    _addCleanupTask_
    
    Create a cleanup task to delete files produces by the parent task.
    """
    cleanupTask = parentTask.addTask("CleanupUnmerged%s" % parentOutputModuleName)
    cleanupTask.setTaskType("Cleanup")

    parentTaskCmssw = parentTask.getStep("cmsRun1")
    cleanupTask.setInputReference(parentTaskCmssw, outputModule = parentOutputModuleName)
    cleanupTask.setSplittingAlgorithm("SiblingProcessingBased", files_per_job = 50)
       
    cleanupStep = cleanupTask.makeStep("cleanupUnmerged%s" % parentOutputModuleName)
    cleanupStep.setStepType("DeleteFiles")
    cleanupTask.applyTemplates()
    return

def setupCMSSWEnv(frameworkVersion):
    """
    _setupCMSSWEnv_

    """
    patchRelease = False
    cmsswDir = "cmssw"
    if frameworkVersion.find("patch") > -1:
        patchRelease = True
        cmsswDir = "cmssw-patch"

    releaseBase = os.path.join(cmsPath, scramArch, "cms", cmsswDir,
                               frameworkVersion)
    pythonLib = "%s/python" % releaseBase
    envFile = "%s/cmsswPaths.py" % pythonLib

    if not os.path.exists(pythonLib):
        print "Unable to locate python libs for release: %s" % pythonLib
        sys.exit(1)

    sys.path.append(pythonLib)
        
    if not patchRelease:
        return
    if not os.path.exists(envFile):
        print "Unable to locate env file for patch release: %s" % envFile
        sys.exit(1)

    fp, pathname, description = imp.find_module(
        os.path.basename(envFile).replace(".py", ""),
        [os.path.dirname(envFile)])
    modRef = imp.load_module("AutoLoadCMSSWPathDefinition", fp, pathname, description)
    pythonPaths = getattr(modRef, "cmsswPythonPaths", None)
    if pythonPaths != None:
        sys.path.extend(pythonPaths)
                                                        
    return

def loadConfig(configPath, workloadSandbox):
    """
    _loadConfig_

    """
    cfgBaseName = os.path.basename(configPath).replace(".py", "")
    cfgDirName = os.path.dirname(configPath)
    modPath = imp.find_module(cfgBaseName, [cfgDirName])
    
    loadedConfig = imp.load_module(cfgBaseName, modPath[0],
                                   modPath[1], modPath[2])

    pickledConfigName = os.path.join(os.getcwd(), workloadSandbox, "pickledConfig.pkl")
    pickledConfigHandle = open(pickledConfigName, "w")
    pickle.dump(loadedConfig.process, pickledConfigHandle)
    pickledConfigHandle.close()

    return (loadedConfig, pickledConfigName)

def outputModulesFromConfig(configHandle):
    """
    _outputModulesFromConfig_

    """
    outputModules = {}
    for outputModuleName in configHandle.process.outputModules.keys():
        outputModule = getattr(configHandle.process, outputModuleName)
        if hasattr(outputModule, "dataset"):
            outputModules[outputModuleName] = {"dataTier": str(getattr(outputModule.dataset, "dataTier", None)),
                                               "filterName": str(getattr(outputModule.dataset, "filterName", None))}

    return outputModules

def injectFilesFromDBS(inputFileset, datasetPath):
    """
    _injectFilesFromDBS_

    """
    print "injecting files from %s into %s, please wait..." % (datasetPath, inputFileset.name)
    args={}
    args["url"] = "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
    args["version"] = "DBS_2_0_9"
    args["mode"] = "GET"
    dbsApi = DbsApi(args)
    dbsResults = dbsApi.listFiles(path = datasetPath, retriveList = ["retrive_lumi", "retrive_run"])
    dbsResults = dbsResults[0:10]
    print "  found %d files, inserting into wmbs..." % (len(dbsResults))

    for dbsResult in dbsResults:
        myFile = File(lfn = dbsResult["LogicalFileName"], size = dbsResult["FileSize"],
                      events = dbsResult["NumberOfEvents"], checksums = {"cksum": dbsResult["Checksum"]},
                      locations = "cmssrm.fnal.gov")
        myRun = Run(runNumber = dbsResult["LumiList"][0]["RunNumber"])
        for lumi in dbsResult["LumiList"]:
            myRun.lumis.append(lumi["LumiSectionNumber"])
        myFile.addRun(myRun)
        myFile.create()
        inputFileset.addFile(myFile)
        
    inputFileset.commit()
    inputFileset.markOpen(False)
    return

def doIndent(level):
    myStr = ""
    while level > 0:
        myStr = myStr + " "
        level -= 1

    return myStr

def injectTaskIntoWMBS(specUrl, workflowName, task, inputFileset, indent = 0):
    """
    _injectTaskIntoWMBS_

    """
    print "%sinjecting %s" % (doIndent(indent), task.getPathName())
    print "%s  input fileset: %s" % (doIndent(indent), inputFileset.name)

    myWorkflow = Workflow(spec = specUrl, owner = "sfoulkes@fnal.gov",
                          name = workflowName, task = task.getPathName())
    myWorkflow.create()

    mySubscription = Subscription(fileset = inputFileset, workflow = myWorkflow,
                                  split_algo = task.jobSplittingAlgorithm(),
                                  type = task.taskType())
    mySubscription.create()
    mySubscription.markLocation("T1_US_FNAL")

    outputModules = task.getOutputModulesForTask()
    for outputModule in outputModules:
        for outputModuleName in outputModule.listSections_():
            print "%s  configuring output module: %s" % (doIndent(indent), outputModuleName)
            if task.taskType() == "Merge":
                outputFilesetName = "%s/merged-%s" % (task.getPathName(),
                                                      outputModuleName)
            else:
                outputFilesetName = "%s/unmerged-%s" % (task.getPathName(),
                                                        outputModuleName)

            print "%s    output fileset: %s" % (doIndent(indent), outputFilesetName)
            outputFileset = Fileset(name = outputFilesetName)
            outputFileset.create()

            myWorkflow.addOutput(outputModuleName, outputFileset)

            # See if any other steps run over this output.
            print "%s    searching for child tasks..." % (doIndent(indent))
            for childTask in task.childTaskIterator():
                if childTask.data.input.outputModule == outputModuleName:
                    injectTaskIntoWMBS(specUrl, workflowName, childTask, outputFileset, indent + 4)

def connectToDB(wmagentConfig):
    """
    _connectToDB_

    """
    socketLoc = getattr(wmAgentConfig.CoreDatabase, "socket", None)
    connectUrl = getattr(wmAgentConfig.CoreDatabase, "connectUrl", None)
    (dialect, junk) = connectUrl.split(":", 1)

    myWMInit = WMInit()
    myWMInit.setDatabaseConnection(dbConfig = connectUrl, dialect = dialect,
                                   socketLoc = socketLoc)
    return

if __name__ == "__main__":
    print "Creating sandbox..."
    workloadSandbox = "%s-%s" % (workloadName, processingVersion)
    os.mkdir(workloadSandbox)
    
    wmAgentConfig = loadConfigurationFile(os.environ["WMAGENT_CONFIG"])
    connectToDB(wmAgentConfig)
    couchUrl = wmAgentConfig.JobStateMachine.couchurl
    configCacheDBName = wmAgentConfig.JobStateMachine.configCacheDBName

    print "Loading config..."
    setupCMSSWEnv("CMSSW_3_5_8_patch3")
    (configHandle, pickledConfig) = loadConfig(processingConfig, workloadSandbox)

    print "Adding config to cache..."
    myConfigCache = WMConfigCache(dbname2 = configCacheDBName, dburl = couchUrl)    
    (configDoc, revision) = myConfigCache.addConfig(pickledConfig)
    (configDoc, revision) = myConfigCache.addOriginalConfig(configDoc, revision, processingConfig)
    
    outputModuleInfo = outputModulesFromConfig(configHandle)

    print "Building workload..."
    workload = createWorkload(workloadName)
    procTask = addProcessingTask(workload, "ReReco", inputDatasetName, couchUrl, configCacheDBName, configDoc) 
    addLogCollectTask(procTask)

    for (outputModuleName, datasetInfo) in outputModuleInfo.iteritems():
        addOutputModule(procTask, outputModuleName, datasetInfo["dataTier"],
                        datasetInfo["filterName"])

    print "Creating sandbox..."
    taskMaker = TaskMaker(workload, os.path.join(os.getcwd(), workloadSandbox))
    taskMaker.skipSubscription = True
    taskMaker.processWorkload()

    workloadLocation = os.path.join(os.getcwd(), workloadSandbox, "%s-%s-spec.pkl" % (workloadName, processingVersion))
    workload.save(workloadLocation)

    inputFileset = Fileset(name = procTask.getPathName())
    inputFileset.create()

    injectFilesFromDBS(inputFileset, inputDatasetName)
    injectTaskIntoWMBS(workloadLocation, workloadName, procTask, inputFileset)
