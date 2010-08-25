#!/usr/bin/env python
"""
WorkQueue EndPolicyInterface

"""

__revision__ = "$Id: EndPolicyInterface.py,v 1.1 2009/12/02 13:52:45 swakef Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Policy.PolicyInterface import PolicyInterface

class EndPolicyInterface(PolicyInterface):
    """Interface for end policies"""
    def __init__(self, **args):
        PolicyInterface.__init__(self, **args)

    def __call__(self, *elements):
        """Apply policy to given elements"""
        for ele in elements:
            if ele['ParentQueueId'] != elements[0]['ParentQueueId']:
                msg = "Policy must be applied to elements with the same parent"
                raise RuntimeError, msg
        return self.applyPolicy(*elements)

    def applyPolicy(self, *elements):
        """Apply the given policy"""
        raise NotImplemented
