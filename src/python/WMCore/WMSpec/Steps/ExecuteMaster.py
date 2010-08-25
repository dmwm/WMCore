#!/usr/bin/env python
"""
_ExecuteMaster_

Overseer object that traverses a task and invokes the type based executor
for each step

"""
__author__ = "evansde"
__revision__ = "$Id: ExecuteMaster.py,v 1.15 2010/05/18 20:17:57 mnorman Exp $"
__version__ = "$Revision: 1.15 $"

import threading
import traceback

from WMCore.WMSpec.WMStep import WMStepHelper
import WMCore.WMSpec.Steps.StepFactory as StepFactory
from WMCore.WMSpec.Steps.WMExecutionFailure import WMExecutionFailure
import inspect, os

class ExecuteMaster:
    """
    _ExecuteMaster_

    Traverse the given task and invoke the execute framework
    If an emulator is provided, then invoke the appropriate emulator
    instead of the executor

    """
    def __init__(self):
        pass    



    def __call__(self, task, wmbsJob):
        """
        _operator(task)_

        Load and run executors for all steps in Task, if an emulator is
        available for that step, use it instead.

        """

        myThread = threading.currentThread

        myThread.watchdogMonitor.setupMonitors(task, wmbsJob)

        myThread.watchdogMonitor.notifyJobStart(task)
        
        for step in task.steps().nodeIterator():
            try:
                #myThread.watchdogMonitor.notifyStepStart(step)
                helper = WMStepHelper(step)
                stepType = helper.stepType()
                stepName = helper.name()
                executor = StepFactory.getStepExecutor(stepType)
                self.doExecution(executor, step, wmbsJob)
                myThread.watchdogMonitor.notifyStepEnd(step)
            except Exception, ex:
                msg = "Encountered error while running ExecuteMaster"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.toTaskDirectory()
                logging.error(msg)
                #raise Exception(msg)
                break

        myThread.watchdogMonitor.notifyJobEnd(task)
        return

    def doExecution(self, executor, step, job):
        """
        _doExecution_

        Invoke the Executor for the step provided

        TODO: Add Monitoring thread & setup
        TODO: Exception Handling
        TODO: pre/post outcome can change the next execution task, need to
              ensure that this happens


        """
        myThread = threading.currentThread
        # Tell the watchdog that we're starting the step
        myThread.watchdogMonitor.notifyStepStart(step)

        
        self.toStepDirectory(step)
        executor.initialise(step, job)
        executionObject = executor
        error = False
        if executor.emulationMode:
            executionObject = executor.emulator
        
        
        preOutcome = executionObject.pre()
        if preOutcome != None:
            print "Pre Executor Task Change: %s" % preOutcome
            print "TODO: Implement Me!!!"
            executor.saveReport()
            self.toTaskDirectory()
        try:
            executionObject.execute()
        except WMExecutionFailure, ex:
            executor.diagnostic(ex.code, executor, ExceptionInstance = ex)
            error = True
        #TODO: Handle generic Exception that indicates development/code errors
        executor.saveReport()

        postOutcome = executionObject.post()
        if postOutcome != None:
            print "Pre Executor Task Change: %s" % preOutcome
            print "TODO: Implement Me!!!"
            executor.saveReport()
            self.toTaskDirectory()
        self.toTaskDirectory()

        # Okay, we're done, set the job to successful
        if not error:
            executor.report.setStepStatus(stepName = executor.stepName,
                                          status = 0)
        executor.saveReport()

        # Tell the watchdog that we're done with the step
        myThread.watchdogMonitor.notifyStepEnd(step = step,
                                               stepReport = executor.report)

    def toStepDirectory(self, step):
        """
        _toStepDirectory_

        Switch current working directory to the step location
        within WMTaskSpace

        """
        stepName = WMStepHelper(step).name()
        from WMTaskSpace import taskSpace
        stepSpace = taskSpace.stepSpace(stepName)

        os.chdir(stepSpace.location)



    def toTaskDirectory(self):
        """
        _toTaskDirectory_

        Switch to current working directory to the task location
        within WMTaskSpace

        """
        from WMTaskSpace import taskSpace
        os.chdir(taskSpace.location)
        return




