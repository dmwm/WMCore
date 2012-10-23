"""
The purpose of this test is to track what is happening in the PileupFetcher module.
The test instantiates RelValMC worklow (copied from RelValMC_t test) and calls
the PileupFetcher on its generation task.
"""

import os
import shutil
import unittest

from WMCore.WMSpec.StdSpecs.RelValMC import getTestArguments, relValMCWorkload
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.WorkQueue.WMBSHelper import WMBSHelper

from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore.Database.CMSCouch import CouchServer, Document
from WMCore.WMSpec.Steps.Fetchers.PileupFetcher import PileupFetcher
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
import WMCore.WMSpec.WMTask as WMTask
import WMCore.WMSpec.WMStep as WMStep
from DBSAPI.dbsApi import DbsApi
from WMCore.WMRuntime.SandboxCreator import SandboxCreator
from WMCore.Wrappers.JsonWrapper import JSONDecoder


class PileupFetcherTest(unittest.TestCase):
    def setUp(self):
        """
        Initialize the database and couch.

        """
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setupCouch("pileupfetcher_t", "ConfigCache")
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)
        couchServer = CouchServer(os.environ["COUCHURL"])
        self.configDatabase = couchServer.connectDatabase("pileupfetcher_t")
        self.dirsToErase = []


    def tearDown(self):
        """
        Clear out the database.

        """
        self.testInit.tearDownCouch()
        self.testInit.clearDatabase()
        for path in self.dirsToErase:
            shutil.rmtree(path)


    def _getConfigBase(self):
        """
        The RelValMC workload is supposed to have the same set of config
        values like MonteCarlo, ReReco and PromptSkim workloads combined
        plus three additional config values:
            - GenConfig - ConfigCacheID of the config for the generation task (MonteCarlo)
            - RecoConfig - ConfigCacheID of the config for the reco step (ReReco)
            - AlcaRecoConfig - ConfigCacheID of the config for the skim/alcareco step (PromptSkim)
        this base config values are taken from MonteCarlo_t, ReReco_t, no test for PromptSkim_t

        configurations will be similar, they'll only differ in the output modules they define.

        """
        config = Document()
        config["info"] = None
        config["config"] = None
        config["md5hash"] = "eb1c38cf50e14cf9fc31278a5c8e580f"
        config["pset_hash"] = "7c856ad35f9f544839d8525ca10259a7"
        config["owner"] = {"group": "cmsdataops", "user": "sfoulkes"}
        config["pset_tweak_details"] = None
        return config


    def injectGenerationConfig(self):
        """
        Gen step - Will have one output module, data tier is configurable
        in the workflow.

        """
        config = self._getConfigBase()
        config["pset_tweak_details"] = \
            {"process": {"outputModules_": ["OutputA"],
                         "OutputA": {"dataset": {"filterName": "OutputAFilter",
                                                 "dataTier": "GEN-SIM-RAW"}}}}
        result = self.configDatabase.commitOne(config)
        return result[0]["id"]

    def injectStepOneConfig(self):
        """
        _injectStepOneConfig_

        Will output RAW data.
        """
        config = self._getConfigBase()
        config["pset_tweak_details"] = \
            {"process": {"outputModules_": ["OutputB"],
                         "OutputB": {"dataset": {"filterName": "OutputBFilter",
                                                 "dataTier": "GEN-SIM-RAW"}}}}
        result = self.configDatabase.commitOne(config)
        return result[0]["id"]

    def injectStepTwoConfig(self):
        """
        _injectStepTwoConfig_

        Will output RECO and AOD.
        """
        config = self._getConfigBase()
        config["pset_tweak_details"] = \
            {"process": {"outputModules_": ["OutputC", "OutputD"],
                         "OutputC": {"dataset": {"filterName": "OutputCFilter",
                                                 "dataTier": "GEN-SIM-RECO"}},
                         "OutputD": {"dataset": {"filterName": "OutputDFilter",
                                                 "dataTier": "AODSIM"}}}}
        result = self.configDatabase.commitOne(config)
        return result[0]["id"]


    def injectReconstructionConfig(self):
        """
        Reco step - Will have two output modules. Filter name can be anything,
        data tiers will be "GEN-SIM-RECO" and "ALCARECO".

        """
        config = self._getConfigBase()
        config["pset_tweak_details"] = \
            {"process": {"outputModules_": ["RecoA", "RecoB"],
                         "RecoA": {"dataset": {"filterName": "RecoAFilter",
                                               "dataTier": "GEN-SIM-RECO"}},
                         "RecoB": {"dataset": {"filterName": "RecoBFilter",
                                               "dataTier": "ALCARECO"}}}}
        result = self.configDatabase.commitOne(config)
        return result[0]["id"]


    def injectAlcaRecoConfig(self):
        """
        AlcaReco step configuration, will have two output modules.
        Filter name can be anything, data tiers can be anything.

        """
        config = self._getConfigBase()
        config["pset_tweak_details"] = \
            {"process": {"outputModules_": ["AlcaRecoA", "AlcaRecoB"],
                         "AlcaRecoA": {"dataset": {"filterName": "AlcaRecoAFilter",
                                                   "dataTier": "GEN-SIM-RECO-ALCARECOA"}},
                         "AlcaRecoB": {"dataset": {"filterName": "AlcaRecoBFilter",
                                                   "dataTier": "GEN-SIM-RECO-ALCARECOB"}}}}
        result = self.configDatabase.commitOne(config)
        return result[0]["id"]


    def _queryAndCompareWithDBS(self, pileupDict, defaultArguments, dbsUrl):
        """
        pileupDict is a Python dictionary containing particular pileup
        configuration information. Query DBS on given dataset contained
        now in both input defaultArguments as well as in the pileupDict
        and compare values.

        """
        args = {}
        # this should have been set in CMSSWStepHelper along with
        # the pileup configuration
        args["url"] = dbsUrl
        args["version"] = "DBS_2_0_9"
        args["mode"] = "GET"
        dbsApi = DbsApi(args)

        inputArgs = defaultArguments["PileupConfig"]

        self.assertEqual(len(inputArgs), len(pileupDict),
                         "Number of pileup types different.")
        for pileupType in inputArgs:
            m = ("pileup type '%s' not in PileupFetcher-produced pileup "
                 "configuration: '%s'" % (pileupType, pileupDict))
            self.failUnless(pileupType in pileupDict, m)

        # now query DBS for compare actual results on files lists for each
        # pileup type and dataset and location (storage element names)
        # pileupDict is saved in the file and now comparing items of this
        # configuration with actual DBS results, the structure of pileupDict:
        #    {"pileupTypeA": {"BlockA": {"FileList": [], "StorageElementNames": []},
        #                     "BlockB": {"FileList": [], "StorageElementName": []}, ....}
        for pileupType, datasets  in inputArgs.items():
            # this is from the pileup configuration produced by PileupFetcher
            blockDict = pileupDict[pileupType]
            m = "Number of datasets for pileup type '%s' is not equal." % pileupType
            self.assertEqual(len(blockDict), len(datasets), m)

            for dataset in datasets:
                dbsFileBlocks = dbsApi.listBlocks(dataset = dataset)
                fileList = [] # list of files in the block (dbsFile["LogicalFileName"])
                storageElemNames = [] # list of StorageElementName
                for dbsFileBlock in dbsFileBlocks:
                    blockName = dbsFileBlock["Name"]
                    # each DBS block has a list under 'StorageElementList', iterate over
                    for storElem in dbsFileBlock["StorageElementList"]:
                        storageElemNames.append(storElem["Name"])
                    # now get list of files in the block
                    dbsFiles = dbsApi.listFiles(blockName = blockName)
                    for dbsFile in dbsFiles:
                        fileList.append(dbsFile["LogicalFileName"])
                # now compare the sets:
                m = ("StorageElementNames don't agree for pileup type '%s', "
                     "dataset '%s' in configuration: '%s'" % (pileupType, dataset, pileupDict))
                self.assertEqual(blockDict[blockName]["StorageElementNames"], storageElemNames, m)
                m = ("FileList don't agree for pileup type '%s', dataset '%s' "
                     " in configuration: '%s'" % (pileupType, dataset, pileupDict))
                self.assertEqual(blockDict[blockName]["FileList"], fileList)


    def _queryPileUpConfigFile(self, defaultArguments, task, taskPath):
        """
        Query and compare contents of the the pileup JSON
        configuration files. Iterate over tasks's steps as
        it happens in the PileupFetcher.

        """
        for step in task.steps().nodeIterator():
            helper = WMStep.WMStepHelper(step)
            # returns e.g. instance of CMSSWHelper
            if hasattr(helper.data, "pileup"):
                decoder = JSONDecoder()

                stepPath = "%s/%s" % (taskPath, helper.name())
                pileupConfig = "%s/%s" % (stepPath, "pileupconf.json")
                try:
                    f = open(pileupConfig, 'r')
                    json = f.read()
                    # loads directly the Python dictionary
                    pileupDict =  decoder.decode(json)
                    f.close()
                except IOError:
                    m = "Could not read pileup JSON configuration file: '%s'" % pileupConfig
                    raise RuntimeError(m)
                self._queryAndCompareWithDBS(pileupDict, defaultArguments, helper.data.dbsUrl)


    def testPileupFetcherOnRelValMC(self):
        defaultArguments = getTestArguments()
        defaultArguments["CouchURL"] = os.environ["COUCHURL"]
        defaultArguments["CouchDBName"] = "pileupfetcher_t"
        # in this test, try not to define generation task datatier (first output module
        # should be automatically picked up)
        #defaultArguments["GenDataTier"] = "GEN-SIM-RAW"
        defaultArguments["GenOutputModuleName"] = "OutputA"
        defaultArguments["StepOneOutputModuleName"] = "OutputB"
        defaultArguments["GenConfigCacheID"] = self.injectGenerationConfig()
        defaultArguments["RecoConfigCacheID"] = self.injectReconstructionConfig()
        defaultArguments["AlcaRecoConfigCacheID"] = self.injectAlcaRecoConfig()
        defaultArguments["StepOneConfigCacheID"] = self.injectStepOneConfig()
        defaultArguments["StepTwoConfigCacheID"] = self.injectStepTwoConfig()
        # add pile up information - for the generation task
        defaultArguments["PileupConfig"] = {"cosmics": ["/Mu/PenguinsPenguinsEverywhere-SingleMu-HorriblyJaundicedYellowEyedPenginsSearchingForCarrots-v31/RECO"],
                                            "minbias": ["/Mu/PenguinsPenguinsEverywhere-SingleMu-HorriblyJaundicedYellowEyedPenginsSearchingForCarrots-v31/RECO"]}

        testWorkload = relValMCWorkload("TestWorkload", defaultArguments)
        testWorkload.setSpecUrl("somespec")
        testWorkload.setOwnerDetails("sfoulkes@fnal.gov", "DWMWM")

        # Since this is test of the fetcher - The loading from WMBS isn't
        # really necessary because the fetching happens before the workflow
        # is inserted into WMBS: feed the workload instance directly into fetcher:
        fetcher = PileupFetcher()
        creator = SandboxCreator()
        pathBase = "%s/%s" % ("/tmp", testWorkload.name())
        self.dirsToErase.append(pathBase)
        for topLevelTask in testWorkload.taskIterator():
            for taskNode in topLevelTask.nodeIterator():
                # this is how the call to PileupFetcher is happening
                # from the SandboxCreator test
                task = WMTask.WMTaskHelper(taskNode)
                taskPath = "%s/WMSandbox/%s" % (pathBase, task.name())
                fetcher.setWorkingDirectory(taskPath)
                # create Sandbox for the fetcher ...
                creator._makePathonPackage(taskPath)
                fetcher(task)

                self._queryPileUpConfigFile(defaultArguments, task, taskPath)



if __name__ == "__main__":
    unittest.main()
