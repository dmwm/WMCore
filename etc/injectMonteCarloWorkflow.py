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

arguments = getTestArguments()
arguments["CouchUrl"] = "http://dmwmwriter:PASSWORD@cmssrv52.fnal.gov:5984"
arguments["CouchDBName"] = "config_cache1"
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

myThread = threading.currentThread()
myThread.transaction.begin()
for workloadTask in workload.taskIterator():
    inputFileset = Fileset(name = workloadTask.getPathName())
    inputFileset.create()

    virtualFile = File(lfn = "%s-virtual-input" % workloadTask.getPathName(),
                       size = 0, events = numEvents,
                       locations = "cmssrm.fnal.gov", merged = True)

    myRun = Run(runNumber = 1)
    myRun.lumis.append(1)
    virtualFile.addRun(myRun)
    virtualFile.create()
    inputFileset.addFile(virtualFile)
    inputFileset.commit()

    injectTaskIntoWMBS(os.path.join(os.getcwd(), workloadName, workloadFile),
                       workloadName, workloadTask, inputFileset)

myThread.transaction.commit()
