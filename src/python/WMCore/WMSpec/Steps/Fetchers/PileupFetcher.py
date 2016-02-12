"""
Given a pile up dataset, pull the information required to cache the list
of pileup files in the job sandbox for the dataset.

"""

import os
import datetime
import time
import shutil

import WMCore.WMSpec.WMStep as WMStep
from WMCore.Wrappers.JsonWrapper import JSONEncoder
from WMCore.Services.DBS.DBSReader import DBSReader
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.WMSpec.Steps.Fetchers.FetcherInterface import FetcherInterface
from WMCore.WorkQueue.WorkQueueUtils import makeLocationsList


def mapSitetoPNN(sites):
    """
    Receives a list of site names, query resource control and return
    a list of PNNs.
    """
    from WMCore.ResourceControl.ResourceControl import ResourceControl

    if not len(sites):
        return []

    fakePNNs = []
    rControl = ResourceControl()
    for site in sites:
        fakePNNs.extend(rControl.listSiteInfo(site)['pnn'])
    return fakePNNs


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

        {"pileupTypeA": {"BlockA": {"FileList": [], "PhEDExNodeNames": []},
                         "BlockB": {"FileList": [], "PhEDExNodeName": []}, ....}

        this structure preserves knowledge of where particular files of data
        set are physically (list of SEs) located. DBS only lists sites which
        have all files belonging to blocks but e.g. BlockA of dataset DS1 may
        be located at site1 and BlockB only at site2 - it's possible that only
        a subset of the blocks in a dataset will be at a site.

        """
        # only production PhEDEx is connected (This can be moved to init method
        phedex = PhEDEx()
        node_filter = set(['UNKNOWN', None])
        # convert the siteWhitelist into SE list and add SEs to the pileup location list
        fakePNNs = []
        if fakeSites:
            fakePNNs = mapSitetoPNN(fakeSites)

        resultDict = {}
        # iterate over input pileup types (e.g. "cosmics", "minbias")
        for pileupType in stepHelper.data.pileup.listSections_():
            # the format here is: step.data.pileup.cosmics.dataset = [/some/data/set]
            datasets = getattr(getattr(stepHelper.data.pileup, pileupType), "dataset")
            # each dataset input can generally be a list, iterate over dataset names
            blockDict = {}
            for dataset in datasets:

                blockFileInfo = dbsReader.getFileListByDataset(dataset=dataset, detail=True)

                for fileInfo in blockFileInfo:
                    blockDict.setdefault(fileInfo['block_name'], {'FileList': [],
                                                                  'NumberOfEvents': 0,
                                                                  'PhEDExNodeNames': []})
                    blockDict[fileInfo['block_name']]['FileList'].append(
                        {'logical_file_name': fileInfo['logical_file_name']})
                    blockDict[fileInfo['block_name']]['NumberOfEvents'] += fileInfo['event_count']

                blockReplicasInfo = phedex.getReplicaPhEDExNodesForBlocks(dataset=dataset, complete='y')
                for block in blockReplicasInfo:
                    nodes = set(blockReplicasInfo[block]) - node_filter | set(fakePNNs)
                    blockDict[block]['PhEDExNodeNames'] = list(nodes)
                    blockDict[block]['FileList'] = sorted(blockDict[block]['FileList'])

            resultDict[pileupType] = blockDict
        return resultDict

    def _getCacheFilePath(self, stepHelper):

        fileName = ""
        for pileupType in stepHelper.data.pileup.listSections_():
            datasets = getattr(getattr(stepHelper.data.pileup, pileupType), "dataset")
            fileName += ("_").join(datasets)
        # TODO cache is not very effective if the dataset combination is different between workflow
        # here is possibility of hash value collision
        cacheFile = "%s/pileupconf-%s.json" % (self.cacheDirectory(), hash(fileName))
        return cacheFile

    def _getStepFilePath(self, stepHelper):
        stepPath = "%s/%s" % (self.workingDirectory(), stepHelper.name())
        fileName = "%s/%s" % (stepPath, "pileupconf.json")

        return fileName

    def _writeFile(self, filePath, json):

        directory = filePath.rsplit('/', 1)[0]

        if not os.path.exists(directory):
            os.mkdir(directory)
        try:
            f = open(filePath, 'w')
            f.write(json)
        except IOError:
            m = "Could not save pileup JSON configuration file: '%s'" % filePath
            raise RuntimeError(m)
        finally:
            f.close()

    def _copyFile(self, src, dest):

        directory = dest.rsplit('/', 1)[0]

        if not os.path.exists(directory):
            os.mkdir(directory)
        shutil.copyfile(src, dest)

    def _isCacheExpired(self, cacheFilePath, delta=24):
        """Is the cache expired? At delta hours (default 24) in the future.
        """
        # cache can either be a file name or an already opened file object

        if not os.path.exists(cacheFilePath):
            return True

        delta = datetime.timedelta(hours=delta)
        t = datetime.datetime.now() - delta
        # cache file mtime has been set to cache expiry time
        if os.path.getmtime(cacheFilePath) < time.mktime(t.timetuple()):
            return True

        return False

    def _isCacheValid(self, stepHelper):
        """
        Check whether cache is exits
        TODO: if the cacheDirectory is not inside the Sandbox it should not autormatically deleted.
              We can add cache refresh policy here
        """
        cacheFile = self._getCacheFilePath(stepHelper)

        if not self._isCacheExpired(cacheFile) and os.path.getsize(cacheFile) > 0:
            # if file already exist don't make a new dbs call and overwrite the file.
            # just return
            fileName = self._getStepFilePath(stepHelper)
            if not os.path.isfile(fileName) or os.path.getsize(fileName) != os.path.getsize(cacheFile):
                self._copyFile(cacheFile, fileName)
            return True
        else:
            return False

    def _saveFile(self, stepHelper, json):

        cacheFile = self._getCacheFilePath(stepHelper)
        self._writeFile(cacheFile, json)
        fileName = self._getStepFilePath(stepHelper)
        self._copyFile(cacheFile, fileName)

    def _createPileupConfigFile(self, helper, fakeSites=None):
        """
        Stores pileup JSON configuration file in the working
        directory / sandbox.

        """

        if fakeSites is None:
            fakeSites = []

        if self._isCacheValid(helper):
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
        self._saveFile(helper, json)

    def __call__(self, wmTask):
        """
        Method is called  when WorkQueue creates the sandbox for a job.
        Need to look at the pileup configuration in the spec and query dbs to
        determine the lfns for the files in the datasets and what sites they're
        located at (WQ creates the job sandbox).

        wmTask is instance of WMTask.WMTaskHelper

        """
        fakeSites = []

        # check whether we need to pretend PU data location
        if wmTask.getTrustSitelists():
            fakeSites = makeLocationsList(wmTask.siteWhitelist(), wmTask.siteBlacklist())

        for step in wmTask.steps().nodeIterator():
            helper = WMStep.WMStepHelper(step)
            # returns e.g. instance of CMSSWHelper
            # doesn't seem to be necessary ... strangely (some inheritance involved?)
            # typeHelper = helper.getTypeHelper()
            if hasattr(helper.data, "pileup"):
                self._createPileupConfigFile(helper, fakeSites)
