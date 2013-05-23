#!/usr/bin/env python
"""
_harvestingInjector_

"""

import os
import sys
import threading
import time
from optparse import OptionParser, Option
from copy import copy

from WMCore.WMInit import connectToDB
from WMCore.Configuration import loadConfigurationFile

from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMComponent.DBSBuffer.Database.Interface.DBSBufferFile import DBSBufferFile

from WMCore.DataStructs.Run import Run

from WMCore.WMSpec.StdSpecs.Harvesting import harvestingWorkload, getTestArguments
from DBSAPI.dbsApi import DbsApi

from WMCore.WMSpec.Makers.TaskMaker import TaskMaker
from WMCore.WorkQueue.WMBSHelper import WMBSHelper

def check_list(option, opt, value):
    return value.split(",")

class MyOption(Option):
    TYPES = Option.TYPES + ("list",)
    TYPE_CHECKER = copy(Option.TYPE_CHECKER)
    TYPE_CHECKER["list"] = check_list

usage = "usage: %prog [options]"
parser = OptionParser(usage=usage, option_class=MyOption)
parser.add_option("-d", "--dataset", dest="InputDataset", type="string",
                  action="store", help="Dataset to harvest",
                  metavar="DATASET")
parser.add_option("-R", "--run", dest="RunWhitelist", type="list",
                  action="store", help="Comma separated list of runs",
                  metavar="RUN1,RUN2", default=[])
parser.add_option("-r", "--release", dest="CMSSWVersion", type="string",
                  action="store", help="CMSSW version to use for harvesting",
                  metavar="CMSSW_X_Y_Z")
parser.add_option("-s", "--scenario", dest="Scenario", type="string",
                  action="store", help="Configuration/DataProcessing scenario",
                  metavar="SCENARIO")
parser.add_option("-t", "--global-tag", dest="GlobalTag", type="string",
                  action="store", help="Conditions global tag",
                  metavar="GLOBALTAG")
parser.add_option("-f", "--reference", dest="RefHistogram", type="string",
                  action="store", help="Reference histogram",
                  metavar="LFN")

(options, args) = parser.parse_args()

missing = []
mandatory = ["InputDataset", "CMSSWVersion", "Scenario", "GlobalTag"]
for option in options.__dict__:
    if getattr(options, option) is None and option in mandatory:
        missing.append(option)
if missing:
    print "Error: The following mandatory options are missing:"
    print "\n".join(missing)
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
    print "injecting files from %s into %s, please wait..." % (datasetPath, inputFileset.name)
    args={}
    args["url"] = "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
    args["version"] = "DBS_2_1_1"
    args["mode"] = "GET"
    dbsApi = DbsApi(args)
    dbsResults = dbsApi.listFiles(path = datasetPath, retriveList = ["retrive_lumi", "retrive_run"])
    print "  found %d files, inserting into wmbs..." % (len(dbsResults))

    for dbsResult in dbsResults:
        if runsWhiteList and str(dbsResult["LumiList"][0]["RunNumber"]) not in runsWhiteList:
            continue
        myFile = File(lfn = dbsResult["LogicalFileName"], size = dbsResult["FileSize"],
                      events = dbsResult["NumberOfEvents"], checksums = {"cksum": dbsResult["Checksum"]},
                      locations = "cmssrm.fnal.gov", merged = True)
        myRun = Run(runNumber = dbsResult["LumiList"][0]["RunNumber"])
        for lumi in dbsResult["LumiList"]:
            myRun.lumis.append(lumi["LumiSectionNumber"])
        myFile.addRun(myRun)
        myFile.create()
        inputFileset.addFile(myFile)

    if len(inputFileset) < 1:
        raise Exception, "No files were selected!"

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
    injectFilesFromDBS(inputFileset, inputDatasetPath, options.RunWhitelist)

    myWMBSHelper = WMBSHelper(workload)
    myWMBSHelper._createSubscriptionsInWMBS(workloadTash.getPathName())

myThread.transaction.commit()
