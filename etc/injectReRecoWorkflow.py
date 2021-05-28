#!/usr/bin/env python
"""
_injectReRecoWorkflow_

"""
from __future__ import print_function

import os
import sys
import threading

from WMCore.WMInit import connectToDB
from WMCore.Configuration import loadConfigurationFile

from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMComponent.DBS3Buffer.DBSBufferFile import DBSBufferFile

from WMCore.DataStructs.Run import Run

from WMCore.WMSpec.StdSpecs.ReReco import rerecoWorkload, getTestArguments
from DBSAPI.dbsApi import DbsApi

from WMCore.WMSpec.Makers.TaskMaker import TaskMaker
from WMCore.WorkQueue.WMBSHelper import WMBSHelper

# The default arguments are set in:
#   WMCORE/src/python/WMCore/WMSpec/StdSpecs/ReReco.py
arguments = getTestArguments()
arguments["StdJobSplitAlgo"] = "FileBased"
arguments["StdJobSplitArgs"] = {"files_per_job": 1}
arguments["SkimJobSplitAlgo"] = "FileBased"
arguments["SkimJobSplitArgs"] = {"files_per_job": 1, "include_parents": True}

if len(sys.argv) != 2:
    print("Usage:")
    print("./injectReRecoWorkflow.py PROCESSING_VERSION")
    sys.exit(1)
else:
    arguments["ProcessingVersion"] = sys.argv[1]

connectToDB()

workloadName = "ReReco-%s" % arguments["ProcessingVersion"]
workloadFile = "reReco-%s.pkl" % arguments["ProcessingVersion"]
os.mkdir(workloadName)
workload = rerecoWorkload(workloadName, arguments)
workloadPath = os.path.join(workloadName, workloadFile)
workload.setOwner("sfoulkes@fnal.gov")
workload.setSpecUrl(workloadPath)

# Build a sandbox using TaskMaker
taskMaker = TaskMaker(workload, os.path.join(os.getcwd(), workloadName))
taskMaker.skipSubscription = True
taskMaker.processWorkload()

workload.save(workloadPath)

def injectFilesFromDBS(inputFileset, datasetPath):
    """
    _injectFilesFromDBS_

    """
    print("injecting files from %s into %s, please wait..." % (datasetPath, inputFileset.name))
    args={}
    args["url"] = "https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader"
    args["version"] = "DBS_2_0_9"
    args["mode"] = "GET"
    dbsApi = DbsApi(args)
    dbsResults = dbsApi.listFileArray(path = datasetPath, retriveList = ["retrive_lumi", "retrive_run"])
    dbsResults = dbsResults[0:10]
    print("  found %d files, inserting into wmbs..." % (len(dbsResults)))

    for dbsResult in dbsResults:
        myFile = File(lfn = dbsResult["LogicalFileName"], size = dbsResult["FileSize"],
                      events = dbsResult["NumberOfEvents"], checksums = {"cksum": dbsResult["Checksum"]},
                      locations = "cmssrm.fnal.gov", merged = True)
        myRun = Run(runNumber = dbsResult["LumiList"][0]["RunNumber"])
        for lumi in dbsResult["LumiList"]:
            myRun.appendLumi(lumi["LumiSectionNumber"])
        myFile.addRun(myRun)
        myFile.create()
        inputFileset.addFile(myFile)

        dbsFile = DBSBufferFile(lfn = dbsResult["LogicalFileName"], size = dbsResult["FileSize"],
                                events = dbsResult["NumberOfEvents"], checksums = {"cksum": dbsResult["Checksum"]},
                                locations = "cmssrm.fnal.gov", status = "LOCAL")
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

    myWMBSHelper = WMBSHelper(workload)
    myWMBSHelper._createSubscriptionsInWMBS(workloadTask.getPathName())

myThread.transaction.commit()
