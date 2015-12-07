#!/usr/bin/env python
"""
_injectMonteCarloWorkflow_

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

from WMCore.DataStructs.Run import Run

from WMCore.WMSpec.StdSpecs.MonteCarlo import MonteCarloWorkloadFactory, getTestArguments
from DBSAPI.dbsApi import DbsApi

from WMCore.WMSpec.Makers.TaskMaker import TaskMaker
from WMCore.WorkQueue.WMBSHelper import WMBSHelper

arguments = getTestArguments()
numEvents = None

if len(sys.argv) != 3:
    print "Usage:"
    print "./injectMonteCarloWorkflow.py PROCESSING_VERSION NUM_EVENTS"
    sys.exit(1)
else:
    arguments["ProcessingVersion"] = sys.argv[1]
    numEvents = int(sys.argv[2])

connectToDB()

workloadName = "MonteCarlo-%s" % arguments["ProcessingVersion"]
workloadFile = "MonteCarlo-%s.pkl" % arguments["ProcessingVersion"]
os.mkdir(workloadName)

factory = MonteCarloWorkloadFactory()
workload = factory(workloadName, arguments)
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
