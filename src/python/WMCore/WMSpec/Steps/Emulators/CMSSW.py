#!/usr/bin/env python
"""
_CMSSW_

Basic Emulator for CMSSW Step

"""
from WMCore.WMSpec.Steps.Emulator import Emulator
from WMCore.FwkJobReport.ReportEmu import ReportEmu
from WMCore.WMSpec.WMStep import WMStepHelper

class CMSSW(Emulator):
    """
    _CMSSW_

    Emulate the execution of a CMSSW Step

    """

    def execute(self):
        """
        _emulate_


        """
        self.step.section_("execution")
        self.step.execution.exitStatus = 0
        self.step.emulator.emulatedBy = str(self.__class__.__name__)

        helper = WMStepHelper(self.step)
        cmsswStep = helper.getTypeHelper()
        reportMaker = ReportEmu(Job = self.job, WMStep = cmsswStep)
        self.executor.report = reportMaker()

        # TODO: touch physical file names to make it look like the files are actually
        #       produced during emulation

    def pre(self):
        '''
            _emulatePre_

            Emulates the pre-execution step
        '''
        return self.executor.pre()

    def post(self):
        '''
            _emulatePost_

            Emulates the post-execution step
        '''
        return self.executor.post()
