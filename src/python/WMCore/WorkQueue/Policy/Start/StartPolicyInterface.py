#!/usr/bin/env python
"""
WorkQueue SplitPolicyInterface

"""
__all__ = []

import types

from WMCore.WorkQueue.Policy.PolicyInterface import PolicyInterface
from WMCore.WorkQueue.DataStructs.WorkQueueElement import WorkQueueElement
from WMCore.WorkQueue.WorkQueueUtils import sitesFromStorageEelements
#from WMCore.WorkQueue.DataStructs.CouchWorkQueueElement import CouchWorkQueueElement as WorkQueueElement
from WMCore.WMException import WMException
from WMCore.WorkQueue.WorkQueueExceptions import WorkQueueWMSpecError, WorkQueueNoWorkError
from DBSAPI.dbsApiException import DbsConfigurationError
from WMCore.Services.DBS.DBSErrors import DBSReaderError
from WMCore import Lexicon

class StartPolicyInterface(PolicyInterface):
    """Interface for start policies"""
    def __init__(self, **args):
        PolicyInterface.__init__(self, **args)
        self.workQueueElements = []
        self.wmspec = None
        self.team = None
        self.initialTask = None
        self.splitParams = None
        self.dbs_pool = {}
        self.data = {}
        self.lumi = None
        self.couchdb = None
        self.rejectedWork = [] # List of inputs that were rejected
        self.pileupData = {}

    def split(self):
        """Apply policy to spec"""
        raise NotImplementedError

    def validate(self):
        """Check params and spec are appropriate for the policy"""
        raise NotImplementedError

    def validateCommon(self):
        """Common validation stuff"""
        try:
            Lexicon.requestName(self.wmspec.name())
        except Exception, ex: # can throw many errors e.g. AttributeError, AssertionError etc.
            error = WorkQueueWMSpecError(self.wmspec, "Workflow name validation error: %s" % str(ex))
            raise error

        if self.initialTask.siteWhitelist():
            if type(self.initialTask.siteWhitelist()) in types.StringTypes:
                error = WorkQueueWMSpecError(self.wmspec, 'Invalid site whitelist: Must be tuple/list but is %s' % type(self.initialTask.siteWhitelist()))
                raise error
            try:
                [Lexicon.cmsname(site) for site in self.initialTask.siteWhitelist()]
            except Exception, ex: # can throw many errors e.g. AttributeError, AssertionError etc.
                error = WorkQueueWMSpecError(self.wmspec, "Site whitelist validation error: %s" % str(ex))
                raise error

        if self.initialTask.siteBlacklist():
            if type(self.initialTask.siteBlacklist()) in types.StringTypes:
                error = WorkQueueWMSpecError(self.wmspec, 'Invalid site blacklist: Must be tuple/list but is %s' % type(self.initialTask.siteBlacklist()))
                raise error
            try:
                [Lexicon.cmsname(site) for site in self.initialTask.siteBlacklist()]
            except Exception, ex: # can throw many errors e.g. AttributeError, AssertionError etc.
                error = WorkQueueWMSpecError(self.wmspec, "Site blacklist validation error: %s" % str(ex))
                raise error

        # splitter settings
        if self.args.get('SliceSize', 1) <= 0:
            error = WorkQueueWMSpecError(self.wmspec, 'Zero or negative SliceSize parameter')
            raise error
        if self.args.get('SubSliceSize', 1) <= 0:
            error = WorkQueueWMSpecError(self.wmspec, 'Zero or negative SubSliceSize parameter')
            raise error

        # check input dataset is valid
        try:
            if self.initialTask.getInputDatasetPath():
                Lexicon.dataset(self.initialTask.getInputDatasetPath())
        except Exception, ex: # can throw many errors e.g. AttributeError, AssertionError etc.
            error = WorkQueueWMSpecError(self.wmspec, "Dataset validation error: %s" % str(ex))
            raise error

        # if pileup is found, check that they are valid datasets
        try:
            pileupDatasets = self.wmspec.listPileupDatasets()
            for dbsUrl in pileupDatasets:
                for dataset in pileupDatasets[dbsUrl]:
                    Lexicon.dataset(dataset)
        except Exception, ex: # can throw many errors e.g. AttributeError, AssertionError etc.
            error = WorkQueueWMSpecError(self.wmspec, "Pileup dataset validation error: %s" % str(ex))
            raise error

    def newQueueElement(self, **args):
        # DBS Url may not be available in the initial task
        # but in the pileup data (MC pileup)
        dbsUrl = self.initialTask.dbsUrl()
        if dbsUrl is None and self.pileupData:
            # Get the first DBS found
            dbsUrl = self.wmspec.listPileupDatasets().keys()[0]

        args.setdefault('Status', 'Available')
        args.setdefault('WMSpec', self.wmspec)
        args.setdefault('Task', self.initialTask)
        args.setdefault('RequestName', self.wmspec.name())
        args.setdefault('TaskName', self.initialTask.name())
        args.setdefault('Dbs', dbsUrl)
        args.setdefault('SiteWhitelist', self.initialTask.siteWhitelist())
        args.setdefault('SiteBlacklist', self.initialTask.siteBlacklist())
        args.setdefault('EndPolicy', self.wmspec.endPolicyParameters())
        args.setdefault('Priority', self.wmspec.priority())
        args.setdefault('PileupData', self.pileupData)
        if not args['Priority']:
            args['Priority'] = 0
        ele = WorkQueueElement(**args)
        for data, sites in ele['Inputs'].items():
            if not sites:
                raise WorkQueueWMSpecError(self.wmspec, 'Input data has no locations "%s"' % data)
        # catch infinite splitting loops
        if len(self.workQueueElements) > self.args.get('maxRequestSize', 1e8):
            raise WorkQueueWMSpecError(self.wmspec, 'Too many elements (%d)' % self.args.get('MaxRequestElements', 1e8))
        self.workQueueElements.append(ele)

    def __call__(self, wmspec, task, data = None, mask = None, team = None):
        self.wmspec = wmspec
        # bring in spec specific settings
        self.args.update(self.wmspec.startPolicyParameters())
        self.initialTask = task
        if data:
            self.data = data
        self.mask = mask
        self.validate()
        try:
            pileupDatasets = self.wmspec.listPileupDatasets()
            if pileupDatasets:
                self.pileupData = self.getDatasetLocations(pileupDatasets)
            self.split()
        # For known exceptions raise custom error that will fail the workflow.
        except DbsConfigurationError, ex:
            # A dbs configuration error implies the spec is invalid
            error = WorkQueueWMSpecError(self.wmspec, "DBS config error: %s" % str(ex))
            raise error
        except AssertionError, ex:
            # Assertion generally means validation of an input field failed
            error = WorkQueueWMSpecError(self.wmspec, "Assertion error: %s" % str(ex))
            raise error
        except DBSReaderError, ex:
            # Hacky way of identifying non-existant data, DbsBadRequest chomped by DBSReader
            # DbsConnectionError: Database exception,Invalid parameters thrown by Summary api
            if 'DbsBadRequest' in str(ex) or 'Invalid parameters' in str(ex):
                data = task.data.input.pythonise_() if task.data.input else 'None'
                msg = """data: %s, mask: %s, pileup: %s. %s""" % (str(data), str(mask), str(pileupDatasets), str(ex))
                error = WorkQueueNoWorkError(self.wmspec, msg)
                raise error
            raise # propagate other dbs errors

        # if we have no elements then there was no work in the spec, fail it
        if not self.workQueueElements:
            data = task.data.input.pythonise_() if task.data.input else 'None'
            msg = """data: %s, mask: %s.""" % (str(data), str(mask))
            error = WorkQueueNoWorkError(self.wmspec, msg)
            raise error
        return self.workQueueElements, self.rejectedWork

    def dbs(self, dbs_url = None):
        """Get DBSReader"""
        from WMCore.WorkQueue.WorkQueueUtils import get_dbs
        if dbs_url is None:
            dbs_url = self.initialTask.dbsUrl()
        return get_dbs(dbs_url)

    @staticmethod
    def supportsWorkAddition():
        """Indicates if a given policy supports addition of new work"""
        return False

    def modifyPolicyForWorkAddition(self, inboxElement):
        """Set modifiers to the policy based on the inboxElement information so that after a splitting pass
        with this policy strictly new work is returned, the inbox element must have information
        about already existing work"""
        raise NotImplementedError("This can't be called on a base StartPolicyInterface object")

    def newDataAvailable(self, task, inbound):
        """
            Returns True if there is data in the future could be included as an element
            for the inbound parent. However it doesn't guarantee that the new data
            will be included if the inbound element is split (i.e. the new data could be open blocks for the Block policy).
        """
        raise NotImplementedError("This can't be called on a base StartPolicyInterface object")

    def getDatasetLocations(self, datasets):
        """Returns a dictionary with the location of the datasets according to DBS"""
        result = {}
        for dbsUrl in datasets:
            dbs = self.dbs(dbsUrl)
            for datasetPath in datasets[dbsUrl]:
                locations = dbs.listDatasetLocation(datasetPath)
                result[datasetPath] = locations
        return result
