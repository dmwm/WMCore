#!/usr/bin/env python
"""
_Executor_

Interface definition for a step executor


"""
from __future__ import absolute_import

import json
import logging
import os
import subprocess
import sys

from Utils.FileTools import getFullPath
from Utils.Utilities import zipEncodeStr
from WMCore.FwkJobReport.Report import Report
from WMCore.WMSpec.Steps.StepFactory import getStepEmulator
from WMCore.WMSpec.WMStep import WMStepHelper

getStepName = lambda step: WMStepHelper(step).name()
getStepErrorDestination = lambda step: WMStepHelper(step).getErrorDestinationStep()


def getStepSpace(stepName):
    """
    _getStepSpace_

    Util to get the runtime step space.
    This imports dynamic runtime libraries so be careful how
    you use it

    """
    modName = "WMTaskSpace"
    if modName in sys.modules.keys():
        taskspace = sys.modules[modName]
    else:
        try:
            # taskspace = __import__(modName, globals(), locals(), ['taskSpace'], -1)
            taskspace = __import__(modName, globals(), locals(), ['taskSpace'])

        except ImportError as ex:
            msg = "Unable to load WMTaskSpace module:\n"
            msg += str(ex)
            # TODO: Generic ExecutionException...
            raise RuntimeError(msg)

    try:
        stepSpace = taskspace.taskSpace.stepSpace(stepName)
    except Exception as ex:
        msg = "Error retrieving stepSpace from TaskSpace:\n"
        msg += str(ex)
        raise RuntimeError(msg)
    return stepSpace


class Executor(object):
    """
    _Executor_

    Define API for a step during execution

    """

    def __init__(self):
        self.report = None
        self.diagnostic = None
        self.emulator = None
        self.emulationMode = False
        self.step = None
        self.stepName = None
        self.stepSpace = None
        self.task = None
        self.workload = None
        self.job = None
        self.errorDestination = None
        self.logger = logging.getLogger()
        self.logger.info("Steps.Executor logging started")

    def initialise(self, step, job):
        """
        _initialise_


        Initialise the executor attributes

        """
        self.step = step
        self.job = job
        self.stepName = getStepName(self.step)
        self.stepSpace = getStepSpace(self.stepName)
        self.task = self.stepSpace.getWMTask()
        self.workload = self.stepSpace.taskSpace.workload
        self.report = Report(self.stepName)
        self.report.data.task = self.task.name()
        self.report.data.workload = self.stepSpace.taskSpace.workloadName()
        self.report.data.id = job['id']
        self.errorDestination = getStepErrorDestination(self.step)

        self.step.section_("execution")
        self.step.execution.exitStatus = 0
        self.step.execution.reportLocation = "%s/Report.pkl" % (
            self.stepSpace.location,
        )

        # Set overall step status to 1 (failed)
        self.report.setStepStatus(stepName=self.stepName, status=1)

        #  //
        # //  Does the step contain settings for an emulator?
        # //   If so, load it up

        emulatorName = getattr(self.step.emulator, "emulatorName", None)
        if emulatorName != None:
            self.emulator = getStepEmulator(emulatorName)
            self.emulator.initialise(self)
            self.emulationMode = True

        return

    def saveReport(self):
        """
        _saveReport_

        Save the job report

        """
        self.report.persist(self.step.execution.reportLocation)
        return

    def pre(self, emulator=None):
        """
        _pre_

        pre execution checks. Can alter flow of execution by returning
        a different step in the task. If None, then current step will
        be passed to execute.

        TODO: Define better how to switch to different step within the task

        """
        return None

    def execute(self, emulator=None):
        """
        _execute_

        Override behaviour to execute this step type.
        If Emulator is provided, execute the emulator instead.
        Return a framework job report instance

        """
        msg = "WMSpec.Steps.Executor.execute method not overridden in "
        msg += "implementation: %s\n" % self.__class__.__name__
        raise NotImplementedError(msg)

    def post(self, emulator=None):
        """
        _post_

        post execution checks. Can alter flow of execution by returning
        a different step in the task. If None, then the next step in the task
        will be used next.

        TODO: Define better how to switch to different step within the task

        """
        return None

    def setCondorChirpAttrDelayed(self, key, value, compress=False, maxLen=5120):
        """
        _setCondorChirpAttrDelayed_

        Util to call condor_chirp and publish the key/value pair

        """
        if compress:
            value = zipEncodeStr(value, maxLen=maxLen)

        # construct condor_chirp binary location from CONDOR_CONFIG
        # Note: This works when we do not use containers.
        condor_chirp_bin = None
        condor_config = os.getenv('CONDOR_CONFIG', None)
        if condor_config:
            condor_config_dir = os.path.dirname(condor_config)
            condor_chirp_bin = os.path.join(condor_config_dir, 'main/condor/libexec/condor_chirp')

        # If the above fails, look for the executable in the environment
        # This is the usual case for containers
        if not condor_chirp_bin or not os.path.isfile(condor_chirp_bin):
            condor_chirp_bin = getFullPath("condor_chirp")

        if condor_chirp_bin and os.access(condor_chirp_bin, os.X_OK):
            args = [condor_chirp_bin, 'set_job_attr_delayed', key, json.dumps(value)]
            subprocess.call(args)
        else:
            if condor_chirp_bin and not os.access(condor_chirp_bin, os.X_OK):
                msg = 'condor_chirp was found in: %s, but it was not an executable.' % condor_chirp_bin
            else:
                msg = 'condor_chirp was not found in the system.'
            self.logger.warning(msg)

        return
