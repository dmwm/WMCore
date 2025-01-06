"""
Given a pile up dataset, pull the information required to cache the list
of pileup files in the job sandbox for the dataset.

"""
import datetime
import os
import hashlib
import shutil
import time
import logging
from json import JSONEncoder
import WMCore.WMSpec.WMStep as WMStep
from Utils.Patterns import getDomainName
from Utils.Utilities import encodeUnicodeToBytes
from WMCore.Services.DBS.DBSReader import DBSReader
from WMCore.Services.MSUtils.MSUtils import getPileupDocs
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
        self.rucioAcct = "wmcore_pileup"
        self.rucio = None

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
        # first, figure out which instance of MSPileup and Rucio to use
        pileupInstance = getDomainName(dbsReader.dbsURL)
        msPileupUrl = f"https://{pileupInstance}.cern.ch/ms-pileup/data/pileup"
        # FIXME: this juggling with Rucio is tough! We can get away without it,
        # but for that we would have to use testbed MSPileup against Prod Rucio
        if pileupInstance == "cmsweb-prod" or pileupInstance == "cmsweb":
            rucioAuthUrl, rucioUrl = "cms-rucio-auth", "cms-rucio"
        else:
            rucioAuthUrl, rucioUrl = "cms-rucio-auth-int", "cms-rucio-int"
        # initialize Rucio here to avoid this authentication on T0-WMAgent
        self.rucio = Rucio(self.rucioAcct,
                           authUrl=f"https://{rucioAuthUrl}.cern.ch",
                           hostUrl=f"http://{rucioUrl}.cern.ch")

        # iterate over input pileup types (e.g. "cosmics", "minbias")
        for pileupType in stepHelper.data.pileup.listSections_():
            # the format here is: step.data.pileup.cosmics.dataset = [/some/data/set]
            datasets = getattr(getattr(stepHelper.data.pileup, pileupType), "dataset")
            # each dataset input can generally be a list, iterate over dataset names
            blockDict = {}
            for dataset in datasets:
                # using the original dataset, resolve blocks, files and number of events with DBS
                fCounter = 0
                for fileInfo in dbsReader.getFileListByDataset(dataset=dataset, detail=True):
                    blockDict.setdefault(fileInfo['block_name'], {'FileList': [],
                                                                  'NumberOfEvents': 0,
                                                                  'PhEDExNodeNames': []})
                    blockDict[fileInfo['block_name']]['FileList'].append(fileInfo['logical_file_name'])
                    blockDict[fileInfo['block_name']]['NumberOfEvents'] += fileInfo['event_count']
                    fCounter += 1

                logging.info(f"Found {len(blockDict)} blocks in DBS for dataset {dataset} with {fCounter} files")
                self._getDatasetLocation(dataset, blockDict, msPileupUrl)

            resultDict[pileupType] = blockDict
        return resultDict

    def _getDatasetLocation(self, dset, blockDict, msPileupUrl):
        """
        Given a dataset name, query PhEDEx or Rucio and resolve the block location
        :param dset: string with the dataset name
        :param blockDict: dictionary with DBS summary info
        :param msPileupUrl: string with the MSPileup url
        :return: update blockDict in place
        """
        # fetch the pileup configuration from MSPileup
        try:
            queryDict = {'query': {'pileupName': dset},
                                   'filters': ['pileupName', 'customName', 'containerFraction', 'currentRSEs']}
            doc = getPileupDocs(msPileupUrl, queryDict, method='POST')[0]
            msg = f'Pileup dataset {doc["pileupName"]} with:\n\tcustom name: {doc["customName"]},'
            msg += f'\n\tcurrent RSEs: {doc["currentRSEs"]}\n\tand container fraction: {doc["containerFraction"]}'
            logging.info(msg)
        except Exception as ex:
            logging.error(f'Error querying MSPileup for dataset {dset}. Details: {str(ex)}')
            raise ex

        # custom dataset name means there was a container fraction change, use different scope
        puScope = 'cms'
        if doc["customName"]:
            dset = doc["customName"]
            puScope = 'group.wmcore'

        blockReplicas = self.rucio.getBlocksInContainer(container=dset, scope=puScope)
        logging.info(f"Found {len(blockReplicas)} blocks in container {dset} for scope {puScope}")

        # Finally, update blocks present in Rucio with the MSPileup currentRSEs.
        # Blocks not present in Rucio - hence only in DBS - are meant to be removed.
        for blockName in list(blockDict):
            if blockName not in blockReplicas:
                logging.warning(f"Block {blockName} present in DBS but not in Rucio. Removing it.")
                blockDict.pop(blockName)
            else:
                blockDict[blockName]['PhEDExNodeNames'] = doc["currentRSEs"]
        logging.info(f"Final pileup dataset {dset} has a total of {len(blockDict)} blocks.")

    def _getCacheFilePath(self, stepHelper):

        fileName = ""
        for pileupType in stepHelper.data.pileup.listSections_():
            datasets = getattr(getattr(stepHelper.data.pileup, pileupType), "dataset")
            fileName += "_".join(datasets)
        # TODO cache is not very effective if the dataset combination is different between workflow
        cacheHash = hashlib.sha1(encodeUnicodeToBytes(fileName)).hexdigest()
        cacheFile = "%s/pileupconf-%s.json" % (self.cacheDirectory(), cacheHash)
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
        :param helper: WMStepHelper instance
        :return: None
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
