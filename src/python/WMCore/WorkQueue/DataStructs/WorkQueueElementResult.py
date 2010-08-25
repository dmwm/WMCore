"""
WorkQueueElementResult

A dictionary based object meant to represent a WorkQueue block
"""

#Can we re-use WorkQueueElement for this?

__revision__ = "$Id: WorkQueueElementResult.py,v 1.1 2009/12/02 13:52:45 swakef Exp $"
__version__ = "$Revision: 1.1 $"

class WorkQueueElementResult(dict):
    def __init__(self, **kwargs):
        dict.__init__(self)
        self.update(kwargs)

        # add useful params here
        self.setdefault('Status', None)
        self.setdefault('ParentQueueId', None)
        self.setdefault('FailedItems', None)
        self.setdefault('FractionComplete', None)
