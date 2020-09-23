"""
Given a pile up dataset, pull the information required to cache the list
of pileup files in the job sandbox for the dataset.

"""
from __future__ import print_function

import datetime
import os
import shutil
import time
import logging
from json import JSONEncoder
from Utils.Utilities import usingRucio
import WMCore.WMSpec.WMStep as WMStep
from WMCore.Services.DBS.DBSReader import DBSReader
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Services.Rucio.Rucio import Rucio
from WMCore.WMSpec.Steps.Fetchers.FetcherInterface import FetcherInterface


class PileupFetcher(FetcherInterface):
    """
    Pull dataset block/SE : LFN list from DBS for the
    pileup datasets required by the steps in the job.

    Save these maps as files in the sandbox

    """
    def __init__(self):
        """
        Prepare module setup
        """
        super(PileupFetcher, self).__init__()
        if usingRucio():
            # FIXME: find a way to pass the Rucio account name to this fetcher module
            self.rucioAcct = "wmcore_transferor"
            self.rucio = Rucio(self.rucioAcct)
        else:
            self.phedex = PhEDEx()  # this will go away eventually

    def _queryDbsAndGetPileupConfig(self, stepHelper, dbsReader):
        """
        Method iterates over components of the pileup configuration input
        and queries DBS for valid files in the dataset, plus some extra
        information about each file.

        Information is organized at block level, listing all its files,
        number of events in the block, and its data location (to be resolved
        by a different method using either PhEDEx or Rucio), such as:

        {"pileupTypeA": {"BlockA": {"FileList": [], "PhEDExNodeNames": [], "NumberOfEvents": 123},
                         "BlockB": {"FileList": [], "PhEDExNodeName": []}, ....}
        """
        resultDict = {}
        # iterate over input pileup types (e.g. "cosmics", "minbias")
        for pileupType in stepHelper.data.pileup.listSections_():
            # the format here is: step.data.pileup.cosmics.dataset = [/some/data/set]
            datasets = getattr(getattr(stepHelper.data.pileup, pileupType), "dataset")
            # each dataset input can generally be a list, iterate over dataset names
            blockDict = {}
            for dataset in datasets:

                for fileInfo in dbsReader.getFileListByDataset(dataset=dataset, detail=True):
                    blockDict.setdefault(fileInfo['block_name'], {'FileList': [],
                                                                  'NumberOfEvents': 0,
                                                                  'PhEDExNodeNames': []})
                    blockDict[fileInfo['block_name']]['FileList'].append(fileInfo['logical_file_name'])
                    blockDict[fileInfo['block_name']]['NumberOfEvents'] += fileInfo['event_count']

                self._getDatasetLocation(dataset, blockDict)

            resultDict[pileupType] = blockDict
        return resultDict

    def _getDatasetLocation(self, dset, blockDict):
        """
        Given a dataset name, query PhEDEx or Rucio and resolve the block location
        :param dset: string with the dataset name
        :param blockDict: dictionary with DBS summary info
        :return: update blockDict in place
        """
        if usingRucio():
            blockReplicas = self.rucio.getPileupLockedAndAvailable(dset, account=self.rucioAcct)
            for blockName, blockLocation in blockReplicas.viewitems():
                try:
                    blockDict[blockName]['PhEDExNodeNames'] = list(blockLocation)
                except KeyError:
                    logging.warning("Block '%s' present in Rucio but not in DBS", blockName)
        else:
            blockReplicasInfo = self.phedex.getReplicaPhEDExNodesForBlocks(dataset=dset, complete='y')
            for block in blockReplicasInfo:
                try:
                    blockDict[block]['PhEDExNodeNames'] = list(blockReplicasInfo[block])
                except KeyError:
                    logging.warning("Block '%s' does not have any complete PhEDEx replica", block)

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

    def _writeFile(self, filePath, jsonPU):

        directory = filePath.rsplit('/', 1)[0]

        if not os.path.exists(directory):
            os.mkdir(directory)
        try:
            with open(filePath, 'w') as f:
                f.write(jsonPU)
        except IOError:
            m = "Could not save pileup JSON configuration file: '%s'" % filePath
            raise RuntimeError(m)

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

        if not self._isCacheExpired(cacheFile, delta=0.5) and os.path.getsize(cacheFile) > 0:
            # if file already exist don't make a new dbs call and overwrite the file.
            # just return
            fileName = self._getStepFilePath(stepHelper)
            if not os.path.isfile(fileName) or os.path.getsize(fileName) != os.path.getsize(cacheFile):
                self._copyFile(cacheFile, fileName)
            return True
        else:
            return False

    def _saveFile(self, stepHelper, jsonPU):

        cacheFile = self._getCacheFilePath(stepHelper)
        self._writeFile(cacheFile, jsonPU)
        fileName = self._getStepFilePath(stepHelper)
        self._copyFile(cacheFile, fileName)

    def createPileupConfigFile(self, helper):
        """
        Stores pileup JSON configuration file in the working
        directory / sandbox.

        """
        if self._isCacheValid(helper):
            # if file already exist don't make a new dbs call and overwrite the file.
            # just return
            return

        encoder = JSONEncoder()
        # this should have been set in CMSSWStepHelper along with
        # the pileup configuration
        url = helper.data.dbsUrl
        dbsReader = DBSReader(url)

        configDict = self._queryDbsAndGetPileupConfig(helper, dbsReader)

        # create JSON and save into a file
        jsonPU = encoder.encode(configDict)
        self._saveFile(helper, jsonPU)

    def __call__(self, wmTask):
        """
        Method is called  when WorkQueue creates the sandbox for a job.
        Need to look at the pileup configuration in the spec and query dbs to
        determine the lfns for the files in the datasets and what sites they're
        located at (WQ creates the job sandbox).

        wmTask is instance of WMTask.WMTaskHelper

        """
        for step in wmTask.steps().nodeIterator():
            helper = WMStep.WMStepHelper(step)
            # returns e.g. instance of CMSSWHelper
            # doesn't seem to be necessary ... strangely (some inheritance involved?)
            # typeHelper = helper.getTypeHelper()
            if hasattr(helper.data, "pileup"):
                self.createPileupConfigFile(helper)
