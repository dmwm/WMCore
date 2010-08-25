#!/usr/bin/env python
"""
_Step.Executor.StageOut_

Implementation of an Executor for a StageOut step

"""

from WMCore.WMSpec.Steps.Executor import Executor


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


    def execute(self, step, wmbsJob, emulator = None):
        """
        _execute_


        """
        print "Steps.Executors.StageOut.execute called"


    def post(self, step):
        """
        _post_

        Post execution checkpointing

        """
        print "Steps.Executors.StageOut.post called"
        return None


