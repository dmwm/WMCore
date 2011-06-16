#!/usr/bin/env python
"""

SingleShot EndPolicy

"""




from WMCore.WorkQueue.Policy.End.EndPolicyInterface import EndPolicyInterface

class SingleShot(EndPolicyInterface):
    """
    o No Retries
    o Only require a certain fraction of elements to be successful
    """
    def __init__(self, **args):
        EndPolicyInterface.__init__(self, **args)
        self.args.setdefault('SuccessThreshold', 1.0)

    def applyPolicy(self):
        """Apply the given policy"""
        EndPolicyInterface.applyPolicy(self)

        for result in self.results:
            # override status if SuccessThreshold met
            if result['PercentSuccess'] >= (self.args['SuccessThreshold'] * 100):
                result['Status'] = "Done"
