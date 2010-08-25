#!/usr/bin/env python
"""
_injectReRecoWorkflow_

"""

import os
import sys

from WMCore.WMInit import WMInit
from WMCore.Configuration import loadConfigurationFile

from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMComponent.DBSBuffer.Database.Interface.DBSBufferFile import DBSBufferFile

from WMCore.DataStructs.Run import Run

from WMCore.WMSpec.StdSpecs.ReReco import rerecoWorkload
from DBSAPI.dbsApi import DbsApi

from WMCore.WMSpec.Makers.TaskMaker import TaskMaker

arguments = {
    "CmsPath": "/uscmst1/prod/sw/cms",
    "AcquisitionEra": "WMAgentCommissioning10",
    "Requester": "sfoulkes@fnal.gov",
    "InputDataset": "/MinimumBias/Commissioning10-v4/RAW",
    "CMSSWVersion": "CMSSW_3_5_8_patch3",
    "ScramArch": "slc5_ia32_gcc434",
    "ProcessingVersion": "v2scf",
    "SkimInput": "output",
    "GlobalTag": "GR10_P_v4::All",
    
    "ProcessingConfig": "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/rereco_FirstCollisions_MinimumBias_35X.py?revision=1.8",
    "SkimConfig": "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/DataOps/python/prescaleskimmer.py?revision=1.1",
    
    "CouchUrl": "http://dmwmwriter:gutslap!@cmssrv52.fnal.gov:5984",
    "CouchDBName": "wmagent_config_cache",
    "Scenario": ""

#     "Scenario": "cosmics",
#     "ProcessingConfig": "",
#     "SkimConfig": ""
    }

if not os.environ.has_key("WMAGENT_CONFIG"):
    print "Please set WMAGENT_CONFIG to point at your WMAgent configuration."
    sys.exit(1)

if len(sys.argv) != 2:
    print "Usage:"
    print "./injectReRecoWorkflow.py PROCESSING_VERSION"
    sys.exit(1)
else:
    arguments["ProcessingVersion"] = sys.argv[1]

wmAgentConfig = loadConfigurationFile(os.environ["WMAGENT_CONFIG"])

if not hasattr(wmAgentConfig, "CoreDatabase"):
    print "Your config is missing the CoreDatabase section."

socketLoc = getattr(wmAgentConfig.CoreDatabase, "socket", None)
connectUrl = getattr(wmAgentConfig.CoreDatabase, "connectUrl", None)
(dialect, junk) = connectUrl.split(":", 1)

myWMInit = WMInit()
myWMInit.setDatabaseConnection(dbConfig = connectUrl, dialect = dialect,
                               socketLoc = socketLoc)

workloadName = "ReReco-%s" % arguments["ProcessingVersion"]
workloadFile = "reReco-%s.pkl" % arguments["ProcessingVersion"]
os.mkdir(workloadName)
workload = rerecoWorkload(workloadName, arguments)

# Build a sandbox using TaskMaker
taskMaker = TaskMaker(workload, os.path.join(os.getcwd(), workloadName))
taskMaker.skipSubscription = True
taskMaker.processWorkload()

workload.save(os.path.join(workloadName, workloadFile))

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
                      locations = "cmssrm.fnal.gov", merged = True)
        myRun = Run(runNumber = dbsResult["LumiList"][0]["RunNumber"])
        for lumi in dbsResult["LumiList"]:
            myRun.lumis.append(lumi["LumiSectionNumber"])
        myFile.addRun(myRun)
        myFile.create()
        inputFileset.addFile(myFile)

        dbsFile = DBSBufferFile(lfn = dbsResult["LogicalFileName"], size = dbsResult["FileSize"],
                                events = dbsResult["NumberOfEvents"], checksums = {"cksum": dbsResult["Checksum"]},
                                locations = "cmssrm.fnal.gov", status = "AlreadyInDBS")
        dbsFile.setDatasetPath(datasetPath)
        dbsFile.setAlgorithm(appName = "cmsRun", appVer = "Unknown", appFam = "Unknown",
                             psetHash = "Unknown", configConfig = "Unknown")
        dbsFile.create()
        
    inputFileset.commit()
    inputFileset.markOpen(False)
    return

for workloadTask in workload.taskIterator():
    inputFileset = Fileset(name = workloadTask.getPathName())
    inputFileset.create()

    inputDataset = workloadTask.inputDataset()
    inputDatasetPath = "/%s/%s/%s" % (inputDataset.primary,
                                      inputDataset.processed,
                                      inputDataset.tier)
    injectFilesFromDBS(inputFileset, inputDatasetPath)

    injectTaskIntoWMBS(os.path.join(os.getcwd(), workloadName, workloadFile),
                       workloadName, workloadTask, inputFileset)
