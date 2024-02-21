#!/usr/bin/env python
"""
WorkQueue SplitPolicyInterface

"""
from builtins import str as newstr, bytes
from future.utils import viewitems


__all__ = []
from WMCore.WorkQueue.Policy.PolicyInterface import PolicyInterface
from WMCore.WorkQueue.DataStructs.WorkQueueElement import WorkQueueElement
from WMCore.DataStructs.LumiList import LumiList
from WMCore.WorkQueue.WorkQueueExceptions import WorkQueueWMSpecError, WorkQueueNoWorkError
from WMCore.Services.MSPileup.MSPileupUtils import getPileupDocs
from dbs.exceptions.dbsClientException import dbsClientException
from WMCore.Services.CRIC.CRIC import CRIC
from WMCore.Services.Rucio.Rucio import Rucio
from WMCore.Services.DBS.DBSErrors import DBSReaderError
from WMCore import Lexicon


class StartPolicyInterface(PolicyInterface):
    """Interface for start policies"""

    def __init__(self, **args):
        # We need to pop this object instance from args because otherwise
        # the super class blows up when doing a deepcopy(args)
        self.rucio = args.pop("rucioObject", None)
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
        self.rejectedWork = []  # List of inputs that were rejected
        self.badWork = []  # list of bad work unit (e.g. without any valid files)
        self.pileupData = {}
        self.cric = CRIC()
        # FIXME: for the moment, it will always use the default value
        self.rucioAcct = self.args.get("rucioAcct", "wmcore_transferor")
        self.rucioAcctPU = self.args.get("rucioAcctPU", "wmcore_pileup")
        if not self.rucio:
            self.rucio = Rucio(self.rucioAcct, configDict={'logger': self.logger})

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
        except Exception as ex:  # can throw many errors e.g. AttributeError, AssertionError etc.
            error = WorkQueueWMSpecError(self.wmspec, "Workflow name validation error: %s" % str(ex))
            raise error

        if self.initialTask.siteWhitelist():
            if isinstance(self.initialTask.siteWhitelist(), (newstr, bytes)):
                error = WorkQueueWMSpecError(self.wmspec, 'Invalid site whitelist: Must be tuple/list but is %s' % type(
                    self.initialTask.siteWhitelist()))
                raise error
            try:
                [Lexicon.cmsname(site) for site in self.initialTask.siteWhitelist()]
            except Exception as ex:  # can throw many errors e.g. AttributeError, AssertionError etc.
                error = WorkQueueWMSpecError(self.wmspec, "Site whitelist validation error: %s" % str(ex))
                raise error
        else:
            error = WorkQueueWMSpecError(self.wmspec, "Site whitelist validation error: Empty site whitelist")
            raise error

        if self.initialTask.siteBlacklist():
            if isinstance(self.initialTask.siteBlacklist(), (newstr, bytes)):
                error = WorkQueueWMSpecError(self.wmspec, 'Invalid site blacklist: Must be tuple/list but is %s' % type(
                    self.initialTask.siteBlacklist()))
                raise error
            try:
                [Lexicon.cmsname(site) for site in self.initialTask.siteBlacklist()]
            except Exception as ex:  # can throw many errors e.g. AttributeError, AssertionError etc.
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
        except Exception as ex:  # can throw many errors e.g. AttributeError, AssertionError etc.
            error = WorkQueueWMSpecError(self.wmspec, "Dataset validation error: %s" % str(ex))
            raise error

        # if pileup is found, check that they are valid datasets
        try:
            pileupDatasets = self.wmspec.listPileupDatasets()
            for dbsUrl in pileupDatasets:
                for dataset in pileupDatasets[dbsUrl]:
                    Lexicon.dataset(dataset)
        except Exception as ex:  # can throw many errors e.g. AttributeError, AssertionError etc.
            error = WorkQueueWMSpecError(self.wmspec, "Pileup dataset validation error: %s" % str(ex))
            raise error

    def newQueueElement(self, **args):
        # DBS Url may not be available in the initial task
        # but in the pileup data (MC pileup)
        dbsUrl = self.initialTask.dbsUrl()
        if dbsUrl is None and self.pileupData:
            # Get the first DBS found
            dbsUrl = next(iter(self.wmspec.listPileupDatasets()))

        args.setdefault('Status', 'Available')
        args.setdefault('WMSpec', self.wmspec)
        args.setdefault('Task', self.initialTask)
        args.setdefault('RequestName', self.wmspec.name())
        args.setdefault('TaskName', self.initialTask.name())
        args.setdefault('Dbs', dbsUrl)
        args.setdefault('SiteWhitelist', self.initialTask.siteWhitelist())
        args.setdefault('SiteBlacklist', self.initialTask.siteBlacklist())
        args.setdefault('StartPolicy', self.wmspec.startPolicy())
        args.setdefault('EndPolicy', self.wmspec.endPolicyParameters())
        args.setdefault('Priority', self.wmspec.priority())
        args.setdefault('PileupData', self.pileupData)
        if not args['Priority']:
            args['Priority'] = 0
        ele = WorkQueueElement(**args)
        for data, sites in viewitems(ele['Inputs']):
            if not sites:
                # we comment out raising exception due to issue-11784 and allow WQ element creation
                # but we would like to monitor when and how often it happens
                self.logger.warning('Input data has no location, spec=%s, data=%s', self.wmspec, data)
                # raise WorkQueueWMSpecError(self.wmspec, 'Input data has no locations "%s"' % data)

        # catch infinite splitting loops
        if len(self.workQueueElements) > self.args.get('maxRequestSize', 1e8):
            raise WorkQueueWMSpecError(self.wmspec, 'Too many elements (%d)' % self.args.get('MaxRequestElements', 1e8))
        self.workQueueElements.append(ele)

    def __call__(self, wmspec, task, data=None, mask=None, team=None, continuous=False, rucioObj=None):
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
            self.logger.debug(f'pileupDatasets: {pileupDatasets}')
            if pileupDatasets:
                # unwrap {"url":[datasets]} structure into list of datasets
                self.pileupData = self.getDatasetLocationsFromMSPileup(pileupDatasets)
            self.split()
        # For known exceptions raise custom error that will fail the workflow.
        except dbsClientException as ex:
            # A dbs configuration error implies the spec is invalid
            error = WorkQueueWMSpecError(self.wmspec, "DBS config error: %s" % str(ex))
            raise error
        except AssertionError as ex:
            # Assertion generally means validation of an input field failed
            error = WorkQueueWMSpecError(self.wmspec, "Assertion error: %s" % str(ex))
            raise error
        except DBSReaderError as ex:
            # Hacky way of identifying non-existant data, DbsBadRequest chomped by DBSReader
            if 'Invalid parameters' in str(ex):
                data = task.data.input.pythonise_() if task.data.input else 'None'
                msg = """data: %s, mask: %s, pileup: %s. %s""" % (str(data), str(mask), str(pileupDatasets), str(ex))
                error = WorkQueueNoWorkError(self.wmspec, msg)
                raise error
            raise  # propagate other dbs errors

        # if we have no new elements and we are not adding work to request
        # already running, then raise exception
        if not self.workQueueElements and not continuous:
            data = task.data.input.pythonise_() if task.data.input else 'None'
            msg = "Failed to add work. Input data: %s, mask: %s." % (str(data), str(mask))
            error = WorkQueueNoWorkError(self.wmspec, msg)
            raise error

        return self.workQueueElements, self.rejectedWork, self.badWork

    def dbs(self, dbs_url=None):
        """Get DBSReader"""
        from WMCore.WorkQueue.WorkQueueUtils import get_dbs
        if dbs_url is None:
            dbs_url = self.initialTask.dbsUrl()
        return get_dbs(dbs_url)

    @staticmethod
    def supportsWorkAddition():
        """Indicates if a given policy supports addition of new work"""
        return False

    def getMaskedBlocks(self, task, dbs, datasetPath):
        """
        Get the blocks which pass the lumi mask restrictions. For each block
        return the list of lumis which were ok (given the lumi mask). The data
        structure returned is the following:
        {
            "block1" : {"file1" : LumiList(), "file5" : LumiList(), ...}
            "block2" : {"file2" : LumiList(), "file7" : LumiList(), ...}
        }
        """
        # Get the task mask as a LumiList object to make operations easier
        maskedBlocks = {}
        taskMask = task.getLumiMask()

        # for performance reasons, we first get all the blocknames
        blocks = [x['block_name'] for x in dbs.dbs.listBlocks(dataset=datasetPath)]

        for block in blocks:
            fileLumis = dbs.dbs.listFileLumis(block_name=block, validFileOnly=1)
            for fileLumi in fileLumis:
                lfn = fileLumi['logical_file_name']
                runNumber = str(fileLumi['run_num'])
                lumis = fileLumi['lumi_section_num']
                fileMask = LumiList(runsAndLumis={runNumber: lumis})
                commonMask = taskMask & fileMask
                if commonMask:
                    maskedBlocks.setdefault(block, {})
                    maskedBlocks[block].setdefault(lfn, LumiList())
                    maskedBlocks[block][lfn] += commonMask

        return maskedBlocks

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

    def getDatasetLocations(self, datasets, account=None):
        """
        Returns a dictionary with the location of the datasets according to Rucio
        The definition of "location" here is a union of all sites holding at least
        part of the dataset (defined by the DATASET grouping).
        :param datasets: list of datasets
        :param account: rucio account to use, if it is not provided we fallback to
        pileup account for backward compatibility
        :return: a dictionary of dataset locations, key'ed by the dataset name
        """
        if not account:
            account = self.rucioAcctPU
        if isinstance(datasets, str):
            datasets = [datasets]
        result = {}
        for datasetPath in datasets:
            msg = f"Fetching Rucio locks for account: {account} and dataset: {datasetPath}"
            self.logger.info(msg)
            locations = self.rucio.getDataLockedAndAvailable(name=datasetPath,
                                                             account=account)
            result[datasetPath] = self.cric.PNNstoPSNs(locations)
        return result
    
    def getDatasetLocationsFromMSPileup(self, datasetsWithDbsURL):
        """
        Returns a dictionary with the location of the datasets according to MSPileup
        :param datasetsWithDbsURL: a dict with the DBS URL as the key, and the associated list of datasets as the value
        """
        
        result = {}
        for dbsUrl, datasets in datasetsWithDbsURL.items():
            pileUpinstance = '-testbed' if 'cmsweb-testbed' in dbsUrl else '-prod'
            msPileupUrl = f'https://cmsweb{pileUpinstance}.cern.ch/ms-pileup/data/pileup'
            self.logger.info(f'Will fetch {len(datasets)} from MSPileup url: {msPileupUrl}')
            for dataset in datasets:
                queryDict = {'query': {'pileupName': dataset},
                            'filters': ['expectedRSEs', 'currentRSEs', 'pileupName', 'containerFraction', 'ruleIds']}
                try:
                    doc = getPileupDocs(msPileupUrl, queryDict, method='POST')[0]
                    currentRSEs = doc['currentRSEs']
                    self.logger.debug(f'Retrieved MSPileup document: {doc}')
                    if len(currentRSEs) == 0:
                        self.logger.warning(f'No RSE has a copy of the desired pileup dataset. Expected RSEs: {doc["expectedRSEs"]}')  
                    result[dataset] = doc['currentRSEs']
                except IndexError:
                    self.logger.warning('Did not find any pileup document for query: %s', queryDict['query'])
                    result[dataset] = []
                except Exception as ex:
                    self.logger.exception('Error getting block location from MSPileup for %s: %s', dataset, str(ex))

        return result

    def blockLocationRucioPhedex(self, blockName):
        """
        Wrapper around Rucio and PhEDEx systems.
        Fetch the current location of the block name (if Rucio,
        also consider the locks made on that block)
        :param blockName: string with the block name
        :return: a list of RSEs
        """
        location = self.rucio.getDataLockedAndAvailable(name=blockName,
                                                        account=self.rucioAcct)
        return location
