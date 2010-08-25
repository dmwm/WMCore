"""
WorkQueueElement

A dictionary based object meant to represent a WorkQueue element
"""

__revision__ = "$Id: WorkQueueElement.py,v 1.2 2009/12/09 17:12:44 swakef Exp $"
__version__ = "$Revision: 1.2 $"

STATES = ('Available', 'Negotiating', 'Acquired',
            'Done', 'Failed', 'Canceled')

class WorkQueueElement(dict):
    """Class to represent a WorkQueue element"""
    def __init__(self, **kwargs):
        dict.__init__(self)

        if kwargs.has_key('Status') and kwargs['Status'] not in STATES:
            msg = 'Invalid Status: %s' % kwargs['Status']
            raise ValueError, msg

        self.update(kwargs)

        self.setdefault('Data', None)
        self.setdefault('ParentData', [])
        self.setdefault('Jobs', None)
        self.setdefault('WMSpecUrl', None)
        self.setdefault('WMSpec', None)
        self.setdefault('Id', None)
        self.setdefault('ChildQueueUrl', None)
        self.setdefault('ParentQueueId', None)
        self.setdefault('InsertTime', None)
        self.setdefault('UpdateTime', None)
        self.setdefault('Priority', None)
        self.setdefault('SubscriptionId', None)
        self.setdefault('Status', None)

    def inEndState(self):
        """Have we finished processing"""
        return self.isComplete() or self.isFailed() or self.isCanceled()

    def isComplete(self):
        return self['Status'] == 'Done'

    def isFailed(self):
        return self['Status'] == 'Failed'

    def isRunning(self):
        return self['Status'] == 'Acquired'

    def isAvailable(self):
        return self['Status'] == 'Available'

    def isCanceled(self):
        return self['Status'] == 'Canceled'
