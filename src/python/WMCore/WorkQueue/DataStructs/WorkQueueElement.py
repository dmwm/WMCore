"""
WorkQueueElement

A dictionary based object meant to represent a WorkQueue element
"""

from hashlib import md5

STATES = ('Available', 'Negotiating', 'Acquired', 'Running',
          'Done', 'Failed', 'CancelRequested', 'Canceled')


def possibleSites(element):
    """
    _possibleSites_

    Checks the site and data restrictions and return a list of possible sites
    to work on this element.
    """
    # check if the whole document was provide
    elem = element.get('WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement', element)

    if elem['NoInputUpdate'] and elem['NoPileupUpdate']:
        return elem['SiteWhitelist']

    commonSites = set(elem['SiteWhitelist']) - set(elem['SiteBlacklist'])

    if elem['Inputs'] and elem['NoInputUpdate'] is False:
        commonSites = commonSites.intersection(set([y for x in elem['Inputs'].values() for y in x]))
    if elem['ParentFlag'] and elem['NoInputUpdate'] is False:
        commonSites = commonSites.intersection(set([y for x in elem['ParentData'].values() for y in x]))
    if elem['PileupData'] and elem['NoPileupUpdate'] is False:
        commonSites = commonSites.intersection(set([y for x in elem['PileupData'].values() for y in x]))

    return list(commonSites)


class WorkQueueElement(dict):
    """Class to represent a WorkQueue element"""

    def __init__(self, **kwargs):
        dict.__init__(self)

        if 'Status' in kwargs and kwargs['Status'] not in STATES:
            msg = 'Invalid Status: %s' % kwargs['Status']
            raise ValueError(msg)

        self.update(kwargs)

        self._id = None

        # XXX If adding or modifying any new parameter which affects the
        # XXX workflow or data run over, the id function must be updated

        self.setdefault('Inputs', {})
        self.setdefault('ProcessedInputs', [])
        self.setdefault('RejectedInputs', [])
        # Some workflows require additional datasets for PileUp
        # Track their locations
        self.setdefault('PileupData', {})
        # both ParentData and ParentFlag is needed in case there Dataset split,
        # even though ParentFlag is True it will have empty ParentData
        self.setdefault('ParentData', {})
        self.setdefault('ParentFlag', False)
        # 0 jobs are valid where we need to accept all blocks (only dqm etc subscriptions will run)
        self.setdefault('Jobs', 0)
        self.setdefault('WMSpec', None)
        self.setdefault('SiteWhitelist', [])
        self.setdefault('SiteBlacklist', [])
        self.setdefault('Dbs', None)
        self.setdefault('Task', None)
        self.setdefault('ParentQueueId', None)
        self.setdefault('Priority', 0)
        self.setdefault('SubscriptionId', None)
        self.setdefault('Status', None)
        self.setdefault('EventsWritten', 0)
        self.setdefault('FilesProcessed', 0)
        self.setdefault('PercentComplete', 0)
        self.setdefault('PercentSuccess', 0)
        self.setdefault('RequestName', None)
        self.setdefault('TaskName', None)
        self.setdefault('TeamName', None)
        self.setdefault('StartPolicy', {})
        self.setdefault('EndPolicy', {})
        self.setdefault('ACDC', {})
        self.setdefault('ChildQueueUrl', None)
        self.setdefault('ParentQueueUrl', None)
        self.setdefault('WMBSUrl', None)
        self.setdefault('NumberOfLumis', 0)
        self.setdefault('NumberOfEvents', 0)
        self.setdefault('NumberOfFiles', 0)
        # Number of files added to WMBS including parent files for this element. used only for monitoring purpose
        self.setdefault('NumOfFilesAdded', 0)
        # Mask used to constrain MC run/lumi ranges
        self.setdefault('Mask', None)
        # is new data being added to the inputs i.e. open block with new files or dataset with new closed blocks?
        self.setdefault('OpenForNewData', False)
        # When was the last time we found new data (not the same as when new data was split), e.g. An open block was found
        self.setdefault('TimestampFoundNewData', 0)
        # Trust initial input dataset location only or not
        self.setdefault('NoInputUpdate', False)
        # Trust initial pileup dataset location only or not
        self.setdefault('NoPileupUpdate', False)
        # set the creation time for wq element, need for sorting
        self.setdefault('CreationTime', 0)
        # set to true when updated from a WorkQueueElementResult
        self.modified = False

    def __to_json__(self, thunker):
        """Strip unthunkable"""
        # result = WorkQueueElement(thunker_encoded_json = True,
        result = dict(thunker_encoded_json=True,
                      type='WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement')
        result['WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement'] = {}
        result['WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement'].update(self)
        # Do we need this 'Subscription' or can we not store this at all?
        result['WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement'].pop('Subscription', None)
        result['WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement'].pop('WMSpec', None)
        #        if self.get('Id'):
        #            result['_id'] = result['WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement'].pop('Id')
        #        if self.get('_rev'):
        #            result['_rev'] = result['WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement'].pop('_rev')
        # result['Mask'] = thunker._thunk(result['Mask'])
        return result

    @property
    def id(self):
        """Generate id for element

        id is deterministic and can be used to identify duplicate elements.
        Calculation only includes fields which affect the workflow and input data.
        Result is an md5 hash of a ';' separated list of:
        workflow name, task name, list of inputs, mask, ACDC info, Dbs instance.

        Parent file info not accounted.

        Example:
        >>> WorkQueueElement(RequestName = 'a', TaskName = 'b').id
        '9ef03a6ad8f16d74fb5ba44df92bf1ef'

        Warning: Any change to this function may prevent identical existing and
        new elements from appearing equivalent, thus in the case of expanding
        work subscriptions work duplication can occur. Care must be taken
        if any modification is made.
        """
        if self._id:
            return self._id
        # Assume md5 is good enough for now
        myhash = md5()
        spacer = ';'  # character not present in any field
        myhash.update(self['RequestName'] + spacer)
        # Task will be None in global inbox
        myhash.update(repr(self['TaskName']) + spacer)
        myhash.update(",".join(sorted(self['Inputs'].keys())) + spacer)
        # Check repr is reproducible - should be
        if self['Mask']:
            myhash.update(",".join(["%s=%s" % (x, y) for x, y in self['Mask'].items()]) + spacer)
        else:
            myhash.update("None" + spacer)
        # Check ACDC is deterministic and all params relevant
        myhash.update(",".join(["%s=%s" % (x, y) for x, y in self['ACDC'].items()]) + spacer)
        myhash.update(repr(self['Dbs']) + spacer)
        self._id = myhash.hexdigest()
        return self._id

    @id.setter
    def id(self, value):
        """Set id - use to override built-in id calculation"""
        self._id = value

    def __from_json__(self, jsondata, thunker):
        """"""
        self.update(jsondata)
        return self

    #        self.update(jsondata['WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement'])
    #        self.pop('type', None)
    #        self.pop('thunker_encoded_json', None)
    ##        self['Id'] = jsondata['_id']
    #        self['_rev'] = jsondata['_rev'] # what to do here???
    #        return self

    def inEndState(self):
        """Have we finished processing"""
        return self.isComplete() or self.isFailed() or self.isCanceled()

    def isComplete(self):
        return self['Status'] == 'Done'

    def isFailed(self):
        return self['Status'] == 'Failed'

    def isRunning(self):
        return self['Status'] == 'Running'

    def isAcquired(self):
        return self['Status'] == 'Acquired'

    def isAvailable(self):
        return self['Status'] == 'Available'

    def isCanceled(self):
        return self['Status'] == 'Canceled'

    def isCancelRequested(self):
        return self['Status'] == 'CancelRequested'

    def updateFromSubscription(self, wmbsStatus):
        """Get subscription status"""
        mapping = {'EventsWritten': 'events_written',
                   'FilesProcessed': 'files_processed',
                   'PercentComplete': 'percent_complete',
                   'PercentSuccess': 'percent_success'}
        for ourkey, wmbskey in mapping.items():
            if wmbskey in wmbsStatus and self[ourkey] != wmbsStatus[wmbskey]:
                self['Modified'] = True
                self[ourkey] = wmbsStatus[wmbskey]

    def updateWithResult(self, progressReport):
        """Take a progress report and update ourself
           Return True if progress updated"""
        progressValues = ('Status', 'EventsWritten', 'FilesProcessed',
                          'PercentComplete', 'PercentSuccess')
        modified = False
        for val in progressValues:
            # ignore if values the same
            if self[val] == progressReport[val]:
                continue
            # ignore new state if it is lower than the current state
            if val == 'Status' and self[val] and STATES.index(progressReport[val]) <= STATES.index(self[val]):
                continue

            self[val] = progressReport[val]
            modified = True
        self.modified = modified

    def statusMetrics(self):
        """Returns the status & performance metrics"""
        return dict(Status=self['Status'],
                    PercentComplete=self['PercentComplete'],
                    PercentSuccess=self['PercentSuccess'])

    def passesSiteRestriction(self, site):
        """
        _passesSiteRestriction_

        Given a site name, checks whether it passes the site and data restrictions.
        """
        # Site list restrictions
        if site in self['SiteBlacklist'] or site not in self['SiteWhitelist']:
            return False

        # input data restrictions (TrustSitelists flag)
        if self['NoInputUpdate'] is False:
            for locations in self['Inputs'].values():
                if site not in locations:
                    return False
            if self['ParentFlag']:
                for locations in self['ParentData'].values():
                    if site not in locations:
                        return False

        # pileup data restrictions (TrustPUSitelists flag)
        if self['NoPileupUpdate'] is False:
            for locations in self['PileupData'].values():
                if site not in locations:
                    return False

        return True
