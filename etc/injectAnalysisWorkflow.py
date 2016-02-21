#!/usr/bin/env python
"""
_injectAnalysisWorkflow_

Create a Analysis workflow and inject it as well as some files into WMBS.
"""
from __future__ import print_function

import os
import sys
import time

from WMCore.WMInit import WMInit
from WMCore.Configuration import loadConfigurationFile

from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow

from WMCore.DataStructs.Run import Run

from WMCore.WMSpec.StdSpecs.Analysis import AnalysisWorkloadFactory
from DBSAPI.dbsApi import DbsApi

from WMCore.WMSpec.Makers.TaskMaker import TaskMaker


arguments = {
            'Username' : "mcinquil",
            'Requestor' : "/C=IT/O=INFN/OU=Personal Certificate/L=Perugia/CN=Mattia Cinquilli",
            "Group" : "CRAB-3 Devel",
            "CMSSWVersion" : "CMSSW_3_8_1",
            "ScramArch" : "slc5_ia32_gcc434",
            "InputDataset" : "/RelValProdTTbar/JobRobot-MC_3XY_V24_JobRobot-v1/GEN-SIM-DIGI-RECO",
            "JobSplittingAlgorithm": "EventBased",
            "JobSplittingArgs": {"events_per_job": 100000},
            "Emulate" : False,
            "CouchUrl" : "http://username:password@crab.pg.infn.it:5984",
            "CouchDBName": "test2",
            "AnalysisConfigCacheDoc" : "746e4e30f9b7545ba4303845ea006003"
        }

if "WMAGENT_CONFIG" not in os.environ:
    print("Please set WMAGENT_CONFIG to point at your WMAgent configuration.")
    sys.exit(1)

if len(sys.argv) != 1:
    arguments["ProcessingVersion"] = sys.argv[1]
else:
    stri=time.ctime().split()
    stri1=stri[2]
    stri2=stri[3].replace(":","")  
    arguments["ProcessingVersion"] =  '%s_%s'%(stri1,stri2)

wmAgentConfig = loadConfigurationFile(os.environ["WMAGENT_CONFIG"])

if not hasattr(wmAgentConfig, "CoreDatabase"):
    print("Your config is missing the CoreDatabase section.")

socketLoc = getattr(wmAgentConfig.CoreDatabase, "socket", None)
connectUrl = getattr(wmAgentConfig.CoreDatabase, "connectUrl", None)
(dialect, junk) = connectUrl.split(":", 1)

myWMInit = WMInit()
myWMInit.setDatabaseConnection(dbConfig = connectUrl, dialect = dialect,
                               socketLoc = socketLoc)

workloadName = "CmsRunAnalysis-%s" % arguments["ProcessingVersion"]
workloadFile = "CmsRunAnalysis-%s.pkl" % arguments["ProcessingVersion"]
os.mkdir(workloadName)

cmsRunAna = AnalysisWorkloadFactory()
workload = cmsRunAna(workloadName, arguments)

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
    print("%sinjecting %s" % (doIndent(indent), task.getPathName()))
    print("%s  input fileset: %s" % (doIndent(indent), inputFileset.name))

    
    myWorkflow = Workflow(spec = specUrl, owner = arguments['Requestor'],
                          name = workflowName, task = task.getPathName())
    myWorkflow.create()


    mySubscription = Subscription(fileset = inputFileset, workflow = myWorkflow,
                                  split_algo = task.jobSplittingAlgorithm(),
                                  type = task.taskType())
    mySubscription.create()

    outputModules =  task.getOutputModulesForStep(task.getTopStepName())
 
    for outputModuleName in outputModules.listSections_():
        print("%s  configuring output module: %s" % (doIndent(indent), outputModuleName))
        if task.taskType() == "Merge":
            outputFilesetName = "%s/merged-%s" % (task.getPathName(),
                                                  outputModuleName)
        else:
            outputFilesetName = "%s/unmerged-%s" % (task.getPathName(),
                                                    outputModuleName)

        print("%s    output fileset: %s" % (doIndent(indent), outputFilesetName))
        outputFileset = Fileset(name = outputFilesetName)
        outputFileset.create()

        myWorkflow.addOutput(outputModuleName, outputFileset)

        # See if any other steps run over this output.
        print("%s    searching for child tasks..." % (doIndent(indent)))
        for childTask in task.childTaskIterator():
            if childTask.data.input.outputModule == outputModuleName:
                injectTaskIntoWMBS(specUrl, workflowName, childTask, outputFileset, indent + 4)                

def injectFilesFromDBS(inputFileset, datasetPath):
    """
    _injectFilesFromDBS_

    """
    print("injecting files from %s into %s, please wait..." % (datasetPath, inputFileset.name))
    args={}
    args["url"] = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
    args["version"] = "DBS_2_0_9"
    args["mode"] = "GET"
    dbsApi = DbsApi(args)
    dbsResults = dbsApi.listFileArray(path = datasetPath, retriveList = ["retrive_block","retrive_lumi", "retrive_run"])

    # NOTE : this is to limit the number of jobs to create ... simply using first 10 files get for the needed dataset
    dbsResults =dbsResults[0:2]


    print("  found %d files, inserting into wmbs..." % (len(dbsResults)))


    for dbsResult in dbsResults:
        myFile = File(lfn = dbsResult["LogicalFileName"], size = dbsResult["FileSize"],
                      events = dbsResult["NumberOfEvents"], checksums = {"cksum": dbsResult["Checksum"]},
                      locations = set(['srm.ciemat.es','storm-se-01.ba.infn.it','storage01.lcg.cscs.ch']))

        myRun = Run(runNumber = dbsResult["LumiList"][0]["RunNumber"])
        for lumi in dbsResult["LumiList"]:
            myRun.lumis.append(lumi["LumiSectionNumber"])
        myFile.addRun(myRun)
        myFile.create()
        inputFileset.addFile(myFile)

    inputFileset.commit()
    inputFileset.markOpen(False)
    return


for workloadTask in workload.taskIterator():
    print("Worload ", workloadTask)
    inputFileset = Fileset(name = workloadTask.getPathName())
    inputFileset.create()

    inputDataset = workloadTask.inputDataset()
    
    inputDatasetPath = "/%s/%s/%s" % (inputDataset.primary,
                                      inputDataset.processed,
                                      inputDataset.tier)
    injectFilesFromDBS(inputFileset, inputDatasetPath)
    injectTaskIntoWMBS(os.path.join(os.getcwd(), workloadName, workloadFile),
                       workloadName, workloadTask, inputFileset)


