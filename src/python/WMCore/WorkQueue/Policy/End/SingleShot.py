#!/usr/bin/env python
"""

SingleShot EndPolicy

"""

__revision__ = "$Id: SingleShot.py,v 1.1 2009/12/02 13:52:45 swakef Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Policy.End.EndPolicyInterface import EndPolicyInterface
from WMCore.WorkQueue.DataStructs.WorkQueueElementResult import WorkQueueElementResult as WQEResult

class SingleShot(EndPolicyInterface):
    """
    o No Retries
    o Only require a certain fraction of elements to be successful
    """
    def __init__(self, **args):
        EndPolicyInterface.__init__(self, **args)
        self.args.setdefault('SuccessThreshold', 0.9)

    def applyPolicy(self, *elements):
        """Apply the given policy"""
        success, running, available = 0, 0, 0
        result = WQEResult(ParentQueueId = elements[0]['ParentQueueId'])
        total = len(elements)
        success = len([x for x in elements if x['Status'] == 'Done'])
        result['FractionComplete'] = success / float(total)
        running = len([x for x in elements if x['Status'] == 'Acquired'])
        available = len([x for x in elements if x['Status'] == 'Available'])

        if available:
            result['Status'] = "Available"
        elif running:
            result['Status'] = "Acquired"
        elif result['FractionComplete'] >= self.args['SuccessThreshold']:
            result['Status'] = "Done"
        else:
            result['Status'] = "Failed"
        return result
