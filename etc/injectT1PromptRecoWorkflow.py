#!/usr/bin/env python
"""
_injectT1PromptRecoReRecoWorkflow_

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

from WMCore.WMSpec.StdSpecs.Tier1PromptReco import tier1promptrecoWorkload, getTestArguments
from DBSAPI.dbsApi import DbsApi

from WMCore.WMSpec.Makers.TaskMaker import TaskMaker
from WMCore.WorkQueue.WMBSHelper import WMBSHelper

# The minimal default arguments are set in:
#   WMCORE/src/python/WMCore/WMSpec/StdSpecs/Tier1PromptReco.py
arguments = getTestArguments()

#Basic arguments
arguments["AcquisitionEra"] = "T0TEST_new_t1PromptReco_BUNNIES"
arguments["Requestor"] = "dballest@fnal.gov"
arguments["CMSSWVersion"] = "CMSSW_5_2_1"

#Dataset related arguments
alcaProducers = ["TkAlCosmics0T","MuAlGlobalCosmics","HcalCalHOCosmics"]
dataTiers = ["AOD", "RECO", "ALCARECO"]

arguments["ProcScenario"] = "cosmics"
arguments["GlobalTag"] = "GR_P_V29::All"
arguments["InputDataset"] = "/Cosmics/Commissioning12-v1/RAW"
arguments["WriteTiers"] = dataTiers
arguments["AlcaSkims"] = alcaProducers

#Site and run whitelists
arguments["runWhitelist"] = [186180]
arguments["siteWhitelist"] = ["T1_US_FNAL"]
arguments["unmergedLFNBase"] = "/store/unmerged"
arguments["mergedLFNBase"] = "/store/backfill/1"

arguments["StdJobSplitAlgo"] = "FileBased"
arguments["StdJobSplitArgs"] = {"files_per_job": 1}

if len(sys.argv) != 2:
    print "Usage:"
    print "./injectReRecoWorkflow.py PROCESSING_VERSION"
    sys.exit(1)
else:
    arguments["ProcessingVersion"] = sys.argv[1]

connectToDB()

workloadName = "t1PromptReco-%s" % arguments["ProcessingVersion"]
workloadFile = "t1PromptReco-%s.pkl" % arguments["ProcessingVersion"]
os.mkdir(workloadName)
workload = tier1promptrecoWorkload(workloadName, arguments)
workloadPath = os.path.join(os.getcwd(), os.path.join(workloadName, workloadFile))
workload.setOwner("dballest@fnal.gov")
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
    myWMBSHelper = WMBSHelper(workload, workloadTask.getPathName(), cachepath=os.getcwd())
    myWMBSHelper._createSubscriptionsInWMBS(workloadTask, inputFileset)

myThread.transaction.commit()
