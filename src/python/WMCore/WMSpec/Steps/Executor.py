#!/usr/bin/env python
"""
_Executor_

Interface definition for a step executor


"""


class Executor:
    """
    _Executor_

    Define API for a step during execution

    """
    
    def __init__(self):
        pass

    def pre(self, step):
        """
        _pre_

        pre execution checks. Can alter flow of execution by returning
        a different step in the task. If None, then current step will
        be passed to execute.

        TODO: Define better how to switch to different step within the task

        """
        return None

    def execute(self, step, wmbsJob, emulator = None):
        """
        _execute_

        Override behaviour to execute this step type.
        If Emulator is provided, execute the emulator instead.
        Return a framework job report instance

        """
        msg = "WMSpec.Steps.Executor.execute method not overridden in "
        msg += "implementation: %s\n" % self.__class__.__name__
        raise NotImplementedError, msg


    def post(self, step):
        """
        _post_

        post execution checks. Can alter flow of execution by returning
        a different step in the task. If None, then the next step in the task
        will be used next.

        TODO: Define better how to switch to different step within the task

        """
        return None


    def installOutcome(self, step, **details):
        """
        _installOutcome_

        Add details on the outcome of the execution to the Step

        """
        step.section_("execution")
        step.execution.exitStatus = details.get("ExitStatus", None)

