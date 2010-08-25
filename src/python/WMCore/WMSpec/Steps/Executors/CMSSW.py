#!/usr/bin/env python
"""
_Step.Executor.CMSSW_

Implementation of an Executor for a CMSSW step

"""


from WMCore.WMSpec.Steps.Executor import Executor


class CMSSW(Executor):
    """
    _CMSWW_

    Execute a CMSSW Step

    """


    def pre(self, step):
        """
        _pre_

        Pre execution checks

        """
        print "Steps.Executors.CMSSW.pre called"
        return None


    def execute(self, step, emulator = None):
        """
        _execute_


        """
        print "Steps.Executors.CMSSW.execute called"


    def post(self, step):
        """
        _post_

        Post execution checkpointing

        """
        print "Steps.Executors.CMSSW.post called"
        return None

