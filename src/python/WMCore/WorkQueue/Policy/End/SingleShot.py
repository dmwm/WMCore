#!/usr/bin/env python
"""

SingleShot EndPolicy

"""




from WMCore.WorkQueue.Policy.End.EndPolicyInterface import EndPolicyInterface

class SingleShot(EndPolicyInterface):
    """
    o No Retries
    """
    def __init__(self, **args):
        EndPolicyInterface.__init__(self, **args)

    def applyPolicy(self):
        """Apply the given policy"""
        EndPolicyInterface.applyPolicy(self)
