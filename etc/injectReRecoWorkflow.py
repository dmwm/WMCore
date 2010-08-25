#!/usr/bin/env python
"""
_injectReRecoWorkflow_

"""

import os
import sys
import threading

from WMCore.WMInit import connectToDB
from WMCore.Configuration import loadConfigurationFile

from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMComponent.DBSBuffer.Database.Interface.DBSBufferFile import DBSBufferFile

from WMCore.DataStructs.Run import Run

from WMCore.WMSpec.StdSpecs.ReReco import rerecoWorkload, getTestArguments
from DBSAPI.dbsApi import DbsApi

from WMCore.WMSpec.Makers.TaskMaker import TaskMaker

# The default arguments are set in:
#   WMCORE/src/python/WMCore/WMSpec/StdSpecs/ReReco.py
arguments = getTestArguments()
arguments["StdJobSplitAlgo"] = "FileBased"
arguments["StdJobSplitArgs"] = {"files_per_job": 1}
arguments["SkimJobSplitAlgo"] = "TwoFileBased"
arguments["SkimJobSplitArgs"] = {"files_per_job": 1}

if len(sys.argv) != 2:
    print "Usage:"
    print "./injectReRecoWorkflow.py PROCESSING_VERSION"
    sys.exit(1)
else:
    arguments["ProcessingVersion"] = sys.argv[1]

connectToDB()

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
                             psetHash = "Unknown", configContent = "Unknown")
        dbsFile.create()
        
    inputFileset.commit()
    inputFileset.markOpen(False)
    return

myThread = threading.currentThread()
myThread.transaction.begin()
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

myThread.transaction.commit()
