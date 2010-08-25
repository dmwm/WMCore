"""
WorkQueueElementResult

A dictionary based object meant to represent a WorkQueue block
"""

#Can we re-use WorkQueueElement for this?

__revision__ = "$Id: WorkQueueElementResult.py,v 1.2 2009/12/09 17:12:44 swakef Exp $"
__version__ = "$Revision: 1.2 $"

class WorkQueueElementResult(dict):
    def __init__(self, **kwargs):
        dict.__init__(self)
        self.update(kwargs)

        # add useful params here
        self.setdefault('Status', None)
        self.setdefault('ParentQueueId', None)
        self.setdefault('WMSpec', None)
        self.setdefault('Elements', [])

    def fractionComplete(self):
        return len(self.completeItems()) / float(len(self['Elements']))

    def completeItems(self):
        return [x for x in self['Elements'] if x.isComplete()]

    def failedItems(self):
        return [x for x in self['Elements'] if x.isFailed()]

    def runningItems(self):
        return [x for x in self['Elements'] if x.isRunning()]

    def availableItems(self):
        return [x for x in self['Elements'] if x.isAvailable()]
