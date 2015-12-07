#!/usr/bin/env python
"""
WorkQueue EndPolicyInterface

"""




from WMCore.WorkQueue.Policy.PolicyInterface import PolicyInterface
from WMCore.WorkQueue.DataStructs.WorkQueueElementResult import WorkQueueElementResult as WQEResult

class EndPolicyInterface(PolicyInterface):
    """Interface for end policies"""
    def __init__(self, **args):
        PolicyInterface.__init__(self, **args)
        self.results = []

    def __call__(self, elements, parents):
        """Apply policy to given elements"""
        for parent in parents:
            elements_for_parent = []
            for element in elements:
                if element['ParentQueueId'] == parent.id:
                    elements_for_parent.append(element)
            result = WQEResult(ParentQueueId = parent.id,
                               ParentQueueElement = parent,
                               Elements = elements_for_parent)
            self.results.append(result)

        self.applyPolicy() # do plugin logic
        return self.results

    def applyPolicy(self):
        """Extend in sub classes for custom behaviour"""
        forceStatus = None
        for result in self.results:
            result['ParentQueueElement'].updateWithResult(result)
            # check for a cancellation request
            if result['ParentQueueElement'].isCancelRequested():
                forceStatus = 'CancelRequested'
        if forceStatus:
            # cancel whole request
            [x.__setitem__('Status', forceStatus) for x in self.results]
