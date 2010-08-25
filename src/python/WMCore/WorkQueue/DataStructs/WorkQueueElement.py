"""
WorkQueueElement

A dictionary based object meant to represent a WorkQueue element
"""

__revision__ = "$Id: WorkQueueElement.py,v 1.1 2009/12/02 13:52:45 swakef Exp $"
__version__ = "$Revision: 1.1 $"

STATES = ('Available', 'Negotiating', 'Acquired',
            'Done', 'Failed', 'Canceled')
END_STATES = ('Done', 'Failed', 'Canceled')

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
        return self['Status'] in END_STATES
