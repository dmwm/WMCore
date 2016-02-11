#!/usr/bin/env python
"""
_LogArchive_

Basic Emulator for LogArchive Step

"""
from __future__ import print_function

import os
import os.path
import re
import shutil

from WMCore.WMSpec.Steps.Emulator import Emulator

class LogArchive(Emulator):
    """
    _LogArchive_

    Emulate the execution of a LogArchive Step

    """
    def pre(self):
        """
        _pre_

        Emulate the stage out pre step

        """
        return None

    def execute(self):
        """
        _execute_

        Emulate LogArchive execution

        """
        self.step.section_("execution")
        self.step.execution.exitStatus = 0
        self.step.section_("emulation")
        self.step.emulation.emulatedBy = str(self.__class__.__name__)

        print("Emulating LogArchive Step")

        #Find all the reports produced in previous steps
        listOfReports = []
        taskLocation = self.stepSpace.taskSpace.location
        task = self.stepSpace.getWMTask()
        for stepName in task.listAllStepNames():
            #First go through all the steps
            step = task.getStep(stepName)
            if step.stepType() == "CMSSW":
                #If we have a CMSSW step, find the report
                stepPath = os.path.join(taskLocation, stepName)
                if 'Report.pkl' in os.listdir(stepPath):
                    shutil.copy(os.path.join(stepPath, 'Report.pkl'), taskLocation)
                    #If we have the report, copy it and exit
                    return
        for dir in os.listdir(taskLocation):
            if not os.path.isdir(os.path.join(taskLocation, dir)):
                continue
            if 'Report.pkl' in os.listdir(os.path.join(taskLocation, dir)):
                listOfReports.append(os.path.join(taskLocation, dir, 'Report.pkl'))

        #For now just move the first Report to the top directory
        if len(listOfReports) > 0:
            shutil.copy(listOfReports[0], taskLocation)


        return



    def post(self):
        """
        _post_

        Emulate post stage out

        """
        return None
