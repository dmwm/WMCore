#!/usr/bin/env python
"""

SingleShot EndPolicy

"""

__revision__ = "$Id: SingleShot.py,v 1.2 2009/12/09 17:12:44 swakef Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WorkQueue.Policy.End.EndPolicyInterface import EndPolicyInterface

class SingleShot(EndPolicyInterface):
    """
    o No Retries
    o Only require a certain fraction of elements to be successful
    """
    def __init__(self, **args):
        EndPolicyInterface.__init__(self, **args)
        self.args.setdefault('SuccessThreshold', 0.9)

    def applyPolicy(self):
        """Apply the given policy"""
        # override status if SuccessThreshold met
        if self.result.fractionComplete() >= self.args['SuccessThreshold']:
            self.result['Status'] = "Done"
