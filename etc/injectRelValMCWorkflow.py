#!/usr/bin/env python

"""

inject script for WMCore/WMSpec/StdSpecs/RelValMC.py workflow
the workflow consists of 3 main subtasks:
    generation (MC)
    reco
    alcareco

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

from WMCore.WMSpec.StdSpecs.RelValMC import relValMCWorkload, getTestArguments
from DBSAPI.dbsApi import DbsApi

from WMCore.WMSpec.Makers.TaskMaker import TaskMaker
from WMCore.WorkQueue.WMBSHelper import WMBSHelper

numEvents = None
# The default arguments are set in:
#    WMCore/WMSpec/StdSpecs/RelValMC.py
arguments = getTestArguments()
#arguments["GenConfigCacheID"] will be taken from CLI
arguments["GenJobSplitAlgo"] = "EventBased"
arguments["GenJobSplitArgs"] = {"events_per_job": 1000}
#arguments["RecoConfigCacheID"] will be taken from CLI
arguments["RecoJobSplitAlgo"] = "FileBased"
arguments["RecoJobSplitArgs"] = {"files_per_job": 1}
#arguments["AlcaRecoConfigCacheID"] will be taken from CLI
arguments["AlcaRecoJobSplitAlgo"] = "FileBased"
arguments["AlcaRecoJobSplitArgs"] = {"files_per_job": 1}


if len(sys.argv) != 6:
    print "Usage:"
    print sys.argv[0], "PROCESSING_VERSION NUM_EVENTS GenConfigCacheID RecoConfigCacheID AlcaRecoConfigCacheID"
    sys.exit(1)
else:
    arguments["ProcessingVersion"] = sys.argv[1]
    numEvents = int(sys.argv[2])
    arguments["GenConfigCacheID"] = sys.argv[3]
    arguments["RecoConfigCacheID"] = sys.argv[4]
    arguments["AlcaRecoConfigCacheID"] = sys.argv[5]

 
connectToDB()

workloadName = "RelValMC-%s" % arguments["ProcessingVersion"]
workloadFile = "relValMC-%s.pkl" % arguments["ProcessingVersion"]
os.mkdir(workloadName)
workload = relValMCWorkload(workloadName, arguments)
workloadPath = os.path.join(workloadName, workloadFile)
workload.setOwner("sfoulkes@fnal.gov")
workload.setSpecUrl(workloadPath)

# Build a sandbox using TaskMaker
taskMaker = TaskMaker(workload, os.path.join(os.getcwd(), workloadName))
taskMaker.skipSubscription = True
taskMaker.processWorkload()

workload.save(workloadPath)


myThread = threading.currentThread()
myThread.transaction.begin()
for workloadTask in workload.taskIterator():
    inputFileset = Fileset(name = workloadTask.getPathName())
    inputFileset.create()
    
    virtualFile = File(lfn = "%s-virtual-input" % workloadTask.getPathName(),
                       size = 0, events = numEvents,
                       locations = set(["cmssrm.fnal.gov", "storm-fe-cms.cr.cnaf.infn.it",
                                        "cmssrm-fzk.gridka.de", "srm2.grid.sinica.edu.tw",
                                        "srm-cms.gridpp.rl.ac.uk", "ccsrm.in2p3.fr",
                                        "srmcms.pic.es"]), merged = False)
    
    myRun = Run(runNumber = 1)
    myRun.lumis.append(1)
    virtualFile.addRun(myRun)
    virtualFile.create()
    inputFileset.addFile(virtualFile)
    inputFileset.commit()

    myWMBSHelper = WMBSHelper(workload)
    myWMBSHelper._createSubscriptionsInWMBS(workloadTask.getPathName())

myThread.transaction.commit()