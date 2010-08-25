#!/usr/bin/env python
"""
_Step.Executor.StageOut_

Implementation of an Executor for a StageOut step

"""

from WMCore.WMSpec.Steps.Executor import Executor
import WMCore.Storage.StageOutMgr as StageOutMgr

class StageOut(Executor):
    """
    _StageOut_

    Execute a StageOut Step

    """        

    def pre(self, step):
        """
        _pre_

        Pre execution checks

        """
        print "Steps.Executors.StageOut.pre called"
        return None


    def execute(self, step, wmbsJob, emulator = None, **overrides):
        """
        _execute_


        """
        # are we faking it?
        if (emulator != None):
            return emulator.emulate( step )
        
        # naw man, this is real
        # iterate over all the incoming files
        manager = StageOutMgr.StageOutMgr(**overrides)
        manager.numberOfRetries = step.retryCount
        manager.retryPauseTime  = step.retryDelay
        
        for currFile in step.files:
            print currFile
            try:
                manager(LFN = currFile.output, PFN = currFile.input)
            except:
                print "Exception raised in stageout executor, how do we handle that?"
                raise
    


    def post(self, step):
        """
        _post_

        Post execution checkpointing

        """
        print "Steps.Executors.StageOut.post called"
        return None


