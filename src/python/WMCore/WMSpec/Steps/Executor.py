#!/usr/bin/env python
"""
_Executor_

Interface definition for a step executor


"""

import sys

from WMCore.FwkJobReport.Report import Report
from WMCore.WMSpec.WMStep import WMStepHelper
from WMCore.WMSpec.Steps.StepFactory import getStepEmulator

getStepName = lambda step: WMStepHelper(step).name()

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
            #taskspace = __import__(modName, globals(), locals(), ['taskSpace'], -1)
            taskspace = __import__(modName, globals(), locals(), ['taskSpace'])
            
        except ImportError, ex:
            msg = "Unable to load WMTaskSpace module:\n"
            msg += str(ex)
            #TODO: Generic ExecutionException...
            raise RuntimeError, msg
        
    try:
        stepSpace = taskspace.taskSpace.stepSpace(stepName)
    except Exception, ex:
        msg = "Error retrieving stepSpace from TaskSpace:\n"
        msg += str(ex)
        raise RuntimeError, msg
    return stepSpace



class Executor:
    """
    _Executor_

    Define API for a step during execution

    """


    def __init__(self):
        self.report = None
        self.diagnostic = None
        self.emulator = None
        self.emulationMode = False
        
    def initialise(self, step, job):
        """
        _initialise_


        Initialise the executor attributes

        """
        self.step = step
        self.job = job
        self.stepName = getStepName(self.step)
        print "stepname %s" % self.stepName
        self.stepSpace = getStepSpace(self.stepName)
        self.task = self.stepSpace.getWMTask()
        self.report = Report(self.stepName)
        self.report.data.task = self.task.name()
        self.report.data.workload = self.stepSpace.taskSpace.workloadName()
        self.report.data.id = job['id']

        self.step.section_("execution")
        self.step.execution.exitStatus = 0
        self.step.execution.reportLocation = "%s/Report.pkl" % (
            self.stepSpace.location,
            )

        # Set overall step status to 1 (failed)
        self.report.setStepStatus(stepName = self.stepName, status = 1)

        #  //
        # //  Does the step contain settings for an emulator?
        #//   If so, load it up
        
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



    def pre(self, emulator = None):
        """
        _pre_

        pre execution checks. Can alter flow of execution by returning
        a different step in the task. If None, then current step will
        be passed to execute.

        TODO: Define better how to switch to different step within the task

        """
        return None

    def execute(self, emulator = None):
        """
        _execute_

        Override behaviour to execute this step type.
        If Emulator is provided, execute the emulator instead.
        Return a framework job report instance

        """
        msg = "WMSpec.Steps.Executor.execute method not overridden in "
        msg += "implementation: %s\n" % self.__class__.__name__
        raise NotImplementedError, msg


    def post(self, emulator = None):
        """
        _post_

        post execution checks. Can alter flow of execution by returning
        a different step in the task. If None, then the next step in the task
        will be used next.

        TODO: Define better how to switch to different step within the task

        """
        return None





