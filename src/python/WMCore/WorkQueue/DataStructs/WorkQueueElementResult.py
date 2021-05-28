"""
WorkQueueElementResult

A dictionary based object meant to represent a WorkQueue block
"""
from __future__ import division

#Can we re-use WorkQueueElement for this?




class WorkQueueElementResult(dict):
    """Class to hold the status of a related group of WorkQueueElements"""
    def __init__(self, **kwargs):
        dict.__init__(self)
        self.update(kwargs)

        self.setdefault('WMSpec', None)
        self.setdefault('Elements', [])
        self.setdefault('Status', self.status())
        if self['Elements']:
            self.setdefault('EventsWritten',
                            sum([x['EventsWritten'] for x in self['Elements']]))
            self.setdefault('FilesProcessed',
                            sum([x['FilesProcessed'] for x in self['Elements']]))
            self.setdefault('Jobs', sum([x['Jobs'] for x in self['Elements']]))
            self.setdefault('PercentComplete',
                            int(sum([x['PercentComplete'] for x in self['Elements']],
                                0.0) / len(self['Elements'])))
            self.setdefault('PercentSuccess',
                            int(sum([x['PercentSuccess'] for x in self['Elements']],
                                0.0) / len(self['Elements'])))
            self.setdefault('RequestName', self['Elements'][0]['RequestName'])
            self.setdefault('TeamName', self['Elements'][0]['TeamName'])
            self.setdefault('Priority', self['Elements'][0]['Priority'])
            self.setdefault('ParentQueueId', self['Elements'][0]['ParentQueueId'])
        elif self.get('ParentQueueElement'):
            self.setdefault('EventsWritten', self['ParentQueueElement']['EventsWritten'])
            self.setdefault('FilesProcessed', self['ParentQueueElement']['FilesProcessed'])
            self.setdefault('Jobs', self['ParentQueueElement']['Jobs'])
            self.setdefault('PercentComplete', self['ParentQueueElement']['PercentComplete'])
            self.setdefault('PercentSuccess', self['ParentQueueElement']['PercentSuccess'])
            self.setdefault('RequestName', self['ParentQueueElement']['RequestName'])
            self.setdefault('TeamName', self['ParentQueueElement']['TeamName'])
            self.setdefault('Priority', self['ParentQueueElement']['Priority'])
            self.setdefault('ParentQueueId', self['ParentQueueElement'].id)
        else:
            raise RuntimeError("Can create WQEResult: No elements or parent provided")

        # some cross checks
        for i in self['Elements']:
            assert(i['RequestName'] == self['RequestName'])
            assert(i['TeamName'] == self['TeamName'])

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

    def acquiredItems(self):
        """Return available items"""
        return [x for x in self['Elements'] if x.isAcquired()]

    def availableItems(self):
        """Return available items"""
        return [x for x in self['Elements'] if x.isAvailable()]

    def canceledItems(self):
        """Return canceled items"""
        return [x for x in self['Elements'] if x.isCanceled()]

    def cancelRequestedItems(self):
        """Return CancelRequested Items"""
        return [x for x in self['Elements'] if x.isCancelRequested()]

    def status(self):
        """Compute status of elements"""
        if not self['Elements'] and self.get('ParentQueueElement'):
            return self['ParentQueueElement']['Status']
        elif not self['Elements']:
            return None

        if not self.inEndState():
            if self.cancelRequestedItems():
                return 'CancelRequested'
            elif self.runningItems():
                return 'Running'
            elif self.acquiredItems() or self.availableItems():
                # available in local queue is acquired to parent
                return 'Acquired'
        else:
            if self.canceledItems():
                return "Canceled"
            elif self.failedItems():
                return "Failed"

        # if all elements have same status take that
        status = self['Elements'][0]['Status']
        for element in self['Elements']:
            if element['Status'] != status:
                msg = "Unable to compute overall status of elements: %s"
                raise RuntimeError(msg % str(self['Elements']))
        return status

    def inEndState(self):
        """Are all requests complete (either success or failure)"""
        if 'Status' in self:
            return self['Status'] in ('Done', 'Failed', 'Canceled')
        for element in self['Elements']:
            if not element.inEndState():
                return False
        return True

    def statusMetrics(self):
        """Returns the status & performance metrics"""
        keys = ['Status', 'PercentComplete', 'PercentSuccess']
        return self.fromkeys(keys)

    def formatForWire(self):
        """Format used to send data over network
        """
        to_remove = ['Elements', 'WMSpec']
        result = dict(self)
        for item in to_remove:
            result.pop(item)
        return result

    def getMaxJobElement(self):
        maxJobElement = self['Elements'][0]
        for x in self['Elements']:
            if x['Jobs'] > maxJobElement['Jobs']:
                maxJobElement = x
        return maxJobElement

    def getMinJobElement(self):
        minJobElement = self['Elements'][0]
        for x in self['Elements']:
            if x['Jobs'] < minJobElement['Jobs']:
                minJobElement = x
        return minJobElement
