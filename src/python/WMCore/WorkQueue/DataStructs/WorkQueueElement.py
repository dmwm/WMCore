"""
WorkQueueElement

A dictionary based object meant to represent a WorkQueue element
"""




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

        self.setdefault('Modified', False)
        self.setdefault('Data', None)
        self.setdefault('ParentData', [])
        self.setdefault('Jobs', None)
        self.setdefault('WMSpecUrl', None)
        self.setdefault('WMSpec', None)
        self.setdefault('Task', None)
        self.setdefault('Id', None)
        self.setdefault('ChildQueueUrl', None)
        self.setdefault('ParentQueueId', None)
        self.setdefault('InsertTime', None)
        self.setdefault('UpdateTime', None)
        self.setdefault('Priority', None)
        self.setdefault('SubscriptionId', None)
        self.setdefault('Status', None)
        self.setdefault('EventsWritten', 0)
        self.setdefault('FilesProcessed', 0)
        self.setdefault('PercentComplete', 0)
        self.setdefault('PercentSuccess', 0)
        self.setdefault('RequestName', None)
        self.setdefault('TeamName', None)

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

    def updateFromSubscription(self, wmbsStatus):
        """Get subscription status"""
        mapping = {'EventsWritten' : 'events_written',
                   'FilesProcessed' : 'files_processed',
                   'PercentComplete' : 'percent_complete',
                   'PercentSuccess' : 'percent_success'}
        for ourkey, wmbskey in mapping.items():
            if wmbsStatus.has_key(wmbskey) and self[ourkey] != wmbsStatus[wmbskey]:
                self['Modified'] = True
                self[ourkey] = wmbsStatus[wmbskey]

    def progressUpdate(self, progressReport):
        """Take a progress report and update ourself
           Return True if progress updated"""
        progressValues = ('EventsWritten', 'FilesProcessed',
                          'PercentComplete', 'PercentSuccess')
        modified = False
        for val in progressValues:
            if self[val] != progressReport[val]:
                self[val] = progressReport[val]
                modified = True
        return modified