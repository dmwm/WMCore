"""
Given a pile up dataset, pull the information required to cache the list
of pileup files in the job sandbox for the dataset.

"""

import os

from WMCore.WMSpec.Steps.Fetchers.FetcherInterface import FetcherInterface
import WMCore.WMSpec.WMStep as WMStep
from WMCore.Wrappers.JsonWrapper import JSONEncoder
from WMCore.Services.DBS.DBSReader import DBSReader

def mapSitetoSE(sites):
    """
    Receives a list of site names, query resource control and return
    a list of SE names.
    """
    from WMCore.ResourceControl.ResourceControl import ResourceControl

    if not len(sites):
        return []

    fakeSEs = []
    rControl = ResourceControl()
    for site in sites:
        fakeSEs.extend(rControl.listSiteInfo(site)['se_name'])
    return fakeSEs

class PileupFetcher(FetcherInterface):
    """
    Pull dataset block/SE : LFN list from DBS for the
    pileup datasets required by the steps in the job.

    Save these maps as files in the sandbox

    """

    def _queryDbsAndGetPileupConfig(self, stepHelper, dbsReader, fakeSites):
        """
        Method iterates over components of the pileup configuration input
        and queries DBS. Then iterates over results from DBS.

        There needs to be a list of files and their locations for each
        dataset name.
        Use dbsReader
        the result data structure is a Python dict following dictionary:
            FileList is a list of LFNs

        {"pileupTypeA": {"BlockA": {"FileList": [], "StorageElementNames": []},
                         "BlockB": {"FileList": [], "StorageElementName": []}, ....}

        this structure preserves knowledge of where particular files of data
        set are physically (list of SEs) located. DBS only lists sites which
        have all files belonging to blocks but e.g. BlockA of dataset DS1 may
        be located at site1 and BlockB only at site2 - it's possible that only
        a subset of the blocks in a dataset will be at a site.

        """
        # convert the siteWhitelist into SE list and add SEs to the pileup location list
        fakeSE = []
        if fakeSites:
            fakeSE = mapSitetoSE(fakeSites)

        resultDict = {}
        # iterate over input pileup types (e.g. "cosmics", "minbias")
        for pileupType in stepHelper.data.pileup.listSections_():
            # the format here is: step.data.pileup.cosmics.dataset = [/some/data/set]
            datasets = getattr(getattr(stepHelper.data.pileup, pileupType), "dataset")
            # each dataset input can generally be a list, iterate over dataset names
            blockDict = {}
            for dataset in datasets:
                blockNames = dbsReader.listFileBlocks(dataset)
                # DBS listBlocks returns list of DbsFileBlock objects for each dataset,
                # iterate over and query each block to get list of files
                for dbsBlockName in blockNames:
                    blockDict[dbsBlockName] = {"FileList": sorted(dbsReader.lfnsInBlock(dbsBlockName)),
                                               "StorageElementNames": dbsReader.listFileBlockLocation(dbsBlockName),
                                               "NumberOfEvents": dbsReader.getDBSSummaryInfo(block=dbsBlockName)['NumberOfEvents']}
                    blockDict[dbsBlockName]['StorageElementNames'].extend(x for x in fakeSE if x not in \
                                                                          blockDict[dbsBlockName]['StorageElementNames'])
            resultDict[pileupType] = blockDict
        return resultDict

    def _createPileupConfigFile(self, helper, fakeSites=None):
        """
        Stores pileup JSON configuration file in the working
        directory / sandbox.

        """

        if fakeSites is None:
            fakeSites = []

        stepPath = "%s/%s" % (self.workingDirectory(), helper.name())
        fileName = "%s/%s" % (stepPath, "pileupconf.json")
        if os.path.isfile(fileName) and os.path.getsize(fileName) > 0:
            # if file already exist don't make a new dbs call and overwrite the file.
            # just return
            return

        encoder = JSONEncoder()
        # this should have been set in CMSSWStepHelper along with
        # the pileup configuration
        url = helper.data.dbsUrl

        dbsReader = DBSReader(url)

        configDict = self._queryDbsAndGetPileupConfig(helper, dbsReader, fakeSites)

        # create JSON and save into a file
        json = encoder.encode(configDict)

        if not os.path.exists(stepPath):
            os.mkdir(stepPath)
        try:
            fileName = "%s/%s" % (stepPath, "pileupconf.json")
            f = open(fileName, 'w')
            f.write(json)
            f.close()
        except IOError:
            m = "Could not save pileup JSON configuration file: '%s'" % fileName
            raise RuntimeError(m)


    def __call__(self, wmTask):
        """
        Method is called  when WorkQueue creates the sandbox for a job.
        Need to look at the pileup configuration in the spec and query dbs to
        determine the lfns for the files in the datasets and what sites they're
        located at (WQ creates the job sandbox).

        wmTask is instance of WMTask.WMTaskHelper

        """
        siteWhitelist = wmTask.siteWhitelist()

        # check whether we need to pretend PU data location
        if wmTask.inputLocationFlag():
            fakeSites = wmTask.siteWhitelist()
        else:
            fakeSites = []

        for step in wmTask.steps().nodeIterator():
            helper = WMStep.WMStepHelper(step)
            # returns e.g. instance of CMSSWHelper
            # doesn't seem to be necessary ... strangely (some inheritance involved?)
            # typeHelper = helper.getTypeHelper()
            if hasattr(helper.data, "pileup"):
                self._createPileupConfigFile(helper, fakeSites)
