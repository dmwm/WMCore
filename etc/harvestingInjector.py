#!/usr/bin/env python
"""
_harvestingInjector_

"""
from __future__ import print_function

import os
import sys
import threading
import time
from argparse import ArgumentParser

from DBSAPI.dbsApi import DbsApi
from WMCore.WMSpec.StdSpecs.Harvesting import harvestingWorkload, getTestArguments

from WMCore.DataStructs.Run import Run
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMInit import connectToDB
from WMCore.WMSpec.Makers.TaskMaker import TaskMaker
from WMCore.WorkQueue.WMBSHelper import WMBSHelper


def check_list(option, opt, value):
    return value.split(",")


def comma_separated_list(string):
    return string.split(',')


usage = "usage: %prog [options]"
parser = ArgumentParser(usage=usage)
parser.add_argument("-d", "--dataset", dest="InputDataset",
                    action="store", help="Dataset to harvest",
                    metavar="DATASET")
parser.add_argument("-R", "--run", dest="RunWhitelist", type=comma_separated_list,
                    action="store", help="Comma separated list of runs",
                    metavar="RUN1,RUN2", default=[])
parser.add_argument("-r", "--release", dest="CMSSWVersion",
                    action="store", help="CMSSW version to use for harvesting",
                    metavar="CMSSW_X_Y_Z")
parser.add_argument("-s", "--scenario", dest="Scenario",
                    action="store", help="Configuration/DataProcessing scenario",
                    metavar="SCENARIO")
parser.add_argument("-t", "--global-tag", dest="GlobalTag",
                    action="store", help="Conditions global tag",
                    metavar="GLOBALTAG")
parser.add_argument("-f", "--reference", dest="RefHistogram",
                    action="store", help="Reference histogram",
                    metavar="LFN")

options = parser.parse_args()

missing = []
mandatory = ["InputDataset", "CMSSWVersion", "Scenario", "GlobalTag"]
for option in options.__dict__:
    if getattr(options, option) is None and option in mandatory:
        missing.append(option)
if missing:
    print("Error: The following mandatory options are missing:")
    print("\n".join(missing))
    sys.exit(1)

# The default arguments are set in:
#   WMCORE/src/python/WMCore/WMSpec/StdSpecs/Harvesting.py
arguments = getTestArguments()
arguments.update(options.__dict__)

connectToDB()

req_time = "%.2f" % time.time()
workloadName = "Harvesting%s--%s" % (arguments["InputDataset"].replace("/", "__"), req_time)
workloadFile = "Harvesting%s--%s.pkl" % (arguments["InputDataset"].replace("/", "__"), req_time)
os.mkdir(workloadName)
workload = harvestingWorkload(workloadName, arguments)
workloadPath = os.path.join(workloadName, workloadFile)
workload.setOwner("sfoulkes@fnal.gov")
workload.setSpecUrl(workloadPath)

# Build a sandbox using TaskMaker
taskMaker = TaskMaker(workload, os.path.join(os.getcwd(), workloadName))
taskMaker.skipSubscription = True
taskMaker.processWorkload()

workload.save(workloadPath)


def injectFilesFromDBS(inputFileset, datasetPath, runsWhiteList=[]):
    """
    _injectFilesFromDBS_

    """
    print("injecting files from %s into %s, please wait..." % (datasetPath, inputFileset.name))
    args = {}
    args["url"] = "https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader"
    args["version"] = "DBS_2_1_1"
    args["mode"] = "GET"
    dbsApi = DbsApi(args)
    dbsResults = dbsApi.listFileArray(path=datasetPath, retriveList=["retrive_lumi", "retrive_run"])
    print("  found %d files, inserting into wmbs..." % (len(dbsResults)))

    for dbsResult in dbsResults:
        if runsWhiteList and str(dbsResult["LumiList"][0]["RunNumber"]) not in runsWhiteList:
            continue
        myFile = File(lfn=dbsResult["LogicalFileName"], size=dbsResult["FileSize"],
                      events=dbsResult["NumberOfEvents"], checksums={"cksum": dbsResult["Checksum"]},
                      locations="cmssrm.fnal.gov", merged=True)
        myRun = Run(runNumber=dbsResult["LumiList"][0]["RunNumber"])
        for lumi in dbsResult["LumiList"]:
            myRun.appendLumi(lumi["LumiSectionNumber"])
        myFile.addRun(myRun)
        myFile.create()
        inputFileset.addFile(myFile)

    if len(inputFileset) < 1:
        raise Exception("No files were selected!")

    inputFileset.commit()
    inputFileset.markOpen(False)
    return


myThread = threading.currentThread()
myThread.transaction.begin()
for workloadTask in workload.taskIterator():
    inputFileset = Fileset(name=workloadTask.getPathName())
    inputFileset.create()

    inputDataset = workloadTask.inputDataset()
    inputDatasetPath = "/%s/%s/%s" % (inputDataset.primary,
                                      inputDataset.processed,
                                      inputDataset.tier)
    injectFilesFromDBS(inputFileset, inputDatasetPath, options.RunWhitelist)

    myWMBSHelper = WMBSHelper(workload)
    myWMBSHelper._createSubscriptionsInWMBS(workloadTash.getPathName())

myThread.transaction.commit()
