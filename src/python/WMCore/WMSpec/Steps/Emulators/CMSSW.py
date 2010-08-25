#!/usr/bin/env python
"""
_CMSSW_

Basic Emulator for CMSSW Step

"""
from WMCore.WMSpec.Steps.Emulator import Emulator

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
        self.step.section_("emulation")
        self.step.emulation.emulatedBy = str(self.__class__.__name__)

        print "Emulating CMSSW Step"
    
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
