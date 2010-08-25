"""
WorkQueueElementResult

A dictionary based object meant to represent a WorkQueue block
"""

#Can we re-use WorkQueueElement for this?

__revision__ = "$Id: WorkQueueElementResult.py,v 1.3 2010/03/02 13:39:41 swakef Exp $"
__version__ = "$Revision: 1.3 $"

class WorkQueueElementResult(dict):
    """Class to hold the status of a related group of WorkQueueElements"""
    def __init__(self, **kwargs):
        dict.__init__(self)
        self.update(kwargs)

        self.setdefault('ParentQueueId', None)
        self.setdefault('WMSpec', None)
        self.setdefault('Elements', [])
        self.setdefault('Status', self.status())

    def fractionComplete(self):
        """Return fraction successful"""
        return len(self.completeItems()) / float(len(self['Elements']))

    def completeItems(self):
        """Return complete items"""
        return [x for x in self['Elements'] if x.isComplete()]

    def failedItems(self):
        """Return failed items"""
        return [x for x in self['Elements'] if x.isFailed()]

    def runningItems(self):
        """Return acquired items"""
        return [x for x in self['Elements'] if x.isRunning()]

    def availableItems(self):
        """Return available items"""
        return [x for x in self['Elements'] if x.isAvailable()]

    def canceledItems(self):
        """Return canceled items"""
        return [x for x in self['Elements'] if x.isCanceled()]

    def status(self):
        """Compute status of elements"""
        if not self['Elements']:
            return None

        if self.availableItems():
            return "Available"
        elif self.runningItems():
            return "Acquired"
        elif self.failedItems():
            return "Failed"
        elif self.canceledItems():
            return "Canceled"
        else:
            # if all elements have same status take that
            status = self['Elements'][0]['Status']
            for element in self['Elements']:
                if element['Status'] != status:
                    msg = "Unable to compute overall status of elements: %s"
                    raise RuntimeError, msg % str(self['Elements'])
            return status
