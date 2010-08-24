"""
WorkQueueElementResult

A dictionary based object meant to represent a WorkQueue block
"""

#Can we re-use WorkQueueElement for this?




class WorkQueueElementResult(dict):
    """Class to hold the status of a related group of WorkQueueElements"""
    def __init__(self, **kwargs):
        dict.__init__(self)
        self.update(kwargs)

        self.setdefault('ParentQueueId', None)
        self.setdefault('WMSpec', None)
        self.setdefault('Elements', [])
        self.setdefault('Status', self.status())
        self.setdefault('EventsWritten',
                        sum([x['EventsWritten'] for x in self['Elements']]))
        self.setdefault('FilesProcessed',
                        sum([x['FilesProcessed'] for x in self['Elements']]))
        self.setdefault('PercentComplete',
                        int(sum([x['PercentComplete'] for x in self['Elements']],
                            0.0) / len(self['Elements'])))
        self.setdefault('PercentSuccess',
                        int(sum([x['PercentSuccess'] for x in self['Elements']],
                            0.0) / len(self['Elements'])))
        self.setdefault('RequestName', self['Elements'][0]['RequestName'])
        self.setdefault('TeamName', self['Elements'][0]['TeamName'])

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

    def inEndState(self):
        """Are all requests complete (either success or failure)"""
        for element in self['Elements']:
            if not element.inEndState():
                return False
        return True

    def formatForWire(self):
        """Format used to send data over network
        """
        to_remove = ['Elements', 'WMSpec']
        result = dict(self)
        for item in to_remove:
            result.pop(item)
        return result