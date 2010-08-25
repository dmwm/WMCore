#!/usr/bin/env python
"""
WorkQueue EndPolicyInterface

"""

__revision__ = "$Id: EndPolicyInterface.py,v 1.3 2010/03/02 13:39:43 swakef Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WorkQueue.Policy.PolicyInterface import PolicyInterface
from WMCore.WorkQueue.DataStructs.WorkQueueElementResult import WorkQueueElementResult as WQEResult

class EndPolicyInterface(PolicyInterface):
    """Interface for end policies"""
    def __init__(self, **args):
        PolicyInterface.__init__(self, **args)
        self.result = None

    def __call__(self, *elements):
        """Apply policy to given elements"""
        self.validate(*elements)

        self.result = WQEResult(ParentQueueId = elements[0]['ParentQueueId'],
                           WMSpec = elements[0]['WMSpec'],
                           Elements = elements)
        self.applyPolicy()
        return self.result

    def applyPolicy(self):
        """Override in sub classes for custom behaviour"""
        pass

    def validate(self, *elements):
        """Valid input?"""
        for ele in elements[1:]:
            if ele['ParentQueueId'] != elements[0]['ParentQueueId']:
                msg = "Policy must be applied to elements with the same parent"
                raise RuntimeError, msg