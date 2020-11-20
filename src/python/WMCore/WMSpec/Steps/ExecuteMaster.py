#!/usr/bin/env python
# pylint: disable=E1101
# E1101: Allow imports from currentThread
"""
_ExecuteMaster_

Overseer object that traverses a task and invokes the type based executor
for each step

"""

import logging
import os
import threading
import traceback

import WMCore.WMSpec.Steps.StepFactory as StepFactory
from WMCore.WMException import WMException
from WMCore.WMSpec.Steps.WMExecutionFailure import WMExecutionFailure
from WMCore.WMSpec.WMStep import WMStepHelper


class ExecuteMaster(object):
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

        try:
            myThread.watchdogMonitor.setupMonitors(task, wmbsJob)
            myThread.watchdogMonitor.notifyJobStart(task)
        except WMException:
            self.toTaskDirectory()
            raise
        except Exception as ex:
            msg = "Encountered unhandled exception while starting monitors:\n"
            msg += str(ex) + '\n'
            msg += str(traceback.format_exc()) + '\n'
            logging.error(msg)
            self.toTaskDirectory()
            raise WMExecutionFailure(msg)

        failureUpstream = False
        for step in task.steps().nodeIterator():
            try:
                helper = WMStepHelper(step)
                stepType = helper.stepType()
                stepName = helper.name()
                if failureUpstream:
                    # for chained steps, don't execute further steps if a
                    # failure has already happened
                    helper.addOverride("previousCmsRunFailure", True)

                executor = StepFactory.getStepExecutor(stepType)
                result = self.doExecution(executor, step, wmbsJob)
                logging.info("StepName: %s, StepType: %s, with result: %r", stepName, stepType, result)
                if result:  # can be either None, or the step exit code
                    failureUpstream = True
            except WMException as ex:
                msg = "Encountered error while running ExecuteMaster:\n"
                msg += str(ex) + "\n"
                logging.error(msg)
                self.toTaskDirectory()
                break
            except Exception as ex:
                msg = "Encountered error while running ExecuteMaster:\n"
                msg += str(ex) + "\n"
                msg += str(traceback.format_exc()) + "\n"
                self.toTaskDirectory()
                logging.error(msg)
                break

        try:
            myThread.watchdogMonitor.notifyJobEnd(task)
        except WMException:
            self.toTaskDirectory()
        except Exception as ex:
            msg = "Encountered unhandled exception while ending the job:\n"
            msg += str(ex) + '\n'
            msg += str(traceback.format_exc()) + '\n'
            logging.error(msg)
            self.toTaskDirectory()

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
        if preOutcome is not None:
            logging.info("Pre Executor Task Change: %s", preOutcome)
            executor.saveReport()
            self.toTaskDirectory()
            myThread.watchdogMonitor.notifyStepEnd(step=step,
                                                   stepReport=executor.report)
            executor.saveReport()
            return preOutcome
        try:
            executor.report.setStepStartTime(stepName=executor.stepName)
            executionObject.execute()
        except WMExecutionFailure as ex:
            executor.diagnostic(ex.code, executor, ExceptionInstance=ex)
            executor.report.addError(executor.stepName, ex.code, "WMAgentStepExecutionError", str(ex))
            error = True
        except Exception as ex:
            logging.error("Exception occured when executing step")
            logging.error("Exception is %s", ex)
            logging.error("Traceback: ")
            logging.error(traceback.format_exc())
            executor.diagnostic(99109, executor, ExceptionInstance=ex)
            executor.report.addError(executor.stepName, 99109, "WMAgentStepExecutionError", str(ex))
            error = True
        executor.report.setStepStopTime(stepName=executor.stepName)
        # TODO: Handle generic Exception that indicates development/code errors
        executor.saveReport()

        postOutcome = executionObject.post()
        if postOutcome is not None:
            logging.info("Post Executor Task Change: %s", postOutcome)
            executor.saveReport()
            self.toTaskDirectory()
            myThread.watchdogMonitor.notifyStepEnd(step=step,
                                                   stepReport=executor.report)
            executor.saveReport()
            return postOutcome

        self.toTaskDirectory()

        # Okay, we're done, set the job to successful
        stepExitCode = executor.report.getExitCode()  # 0 is successful
        if not error and not stepExitCode:
            executor.report.setStepStatus(stepName=executor.stepName,
                                          status=0)
        executor.saveReport()

        # Tell the watchdog that we're done with the step
        myThread.watchdogMonitor.notifyStepEnd(step=step,
                                               stepReport=executor.report)
        executor.saveReport()

        return stepExitCode

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
