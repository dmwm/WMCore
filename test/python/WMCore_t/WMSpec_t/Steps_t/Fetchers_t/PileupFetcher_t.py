"""
_PileupFetcher_t_

The purpose of this test is to track what is happening in the PileupFetcher module.
The test instantiates a MC worklow and calls the PileupFetcher on its generation task.
"""

import os
import unittest

import WMCore.WMSpec.WMStep as WMStep
import WMCore.WMSpec.WMTask as WMTask

from WMCore.Services.DBS.DBSReader import DBSReader
from WMCore.Database.CMSCouch import CouchServer, Document
from WMCore.WMRuntime.SandboxCreator import SandboxCreator
from WMCore.WMSpec.StdSpecs.MonteCarlo import MonteCarloWorkloadFactory
from WMCore.WMSpec.Steps.Fetchers.PileupFetcher import PileupFetcher
from WMCore.Wrappers.JsonWrapper import JSONDecoder
from WMCore.Services.EmulatorSwitch import EmulatorHelper

from WMQuality.TestInitCouchApp import TestInitCouchApp

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
        self.testDir = self.testInit.generateWorkDir()
        EmulatorHelper.setEmulators(dbs = True)

    def tearDown(self):
        """
        Clear out the database.
        """
        self.testInit.tearDownCouch()
        self.testInit.delWorkDir()
        EmulatorHelper.resetEmulators()

    def injectGenerationConfig(self):
        """
        _injectGenerationConfig_

        Inject a generation config for the MC workflow.
        """
        config = Document()
        config["info"] = None
        config["config"] = None
        config["md5hash"] = "eb1c38cf50e14cf9fc31278a5c8e580f"
        config["pset_hash"] = "7c856ad35f9f544839d8525ca10259a7"
        config["owner"] = {"group": "cmsdataops", "user": "sfoulkes"}
        config["pset_tweak_details"] = None
        config["pset_tweak_details"] = \
            {"process": {"outputModules_": ["OutputA"],
                         "OutputA": {"dataset": {"filterName": "OutputAFilter",
                                                 "dataTier": "GEN-SIM-RAW"}}}}
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
        args["version"] = "DBS_2_0_9"
        args["mode"] = "GET"
        reader = DBSReader(dbsUrl, **args)

        inputArgs = defaultArguments["PileupConfig"]

        self.assertEqual(len(inputArgs), len(pileupDict),
                         "Number of pileup types different.")
        for pileupType in inputArgs:
            m = ("pileup type '%s' not in PileupFetcher-produced pileup "
                 "configuration: '%s'" % (pileupType, pileupDict))
            self.assertTrue(pileupType in pileupDict, m)

        # now query DBS for compare actual results on files lists for each
        # pileup type and dataset and location (storage element names)
        # pileupDict is saved in the file and now comparing items of this
        # configuration with actual DBS results, the structure of pileupDict:
        #    {"pileupTypeA": {"BlockA": {"FileList": [], "StorageElementNames": []},
        #                     "BlockB": {"FileList": [], "StorageElementName": []}, ....}
        for pileupType, datasets  in inputArgs.items():
            # this is from the pileup configuration produced by PileupFetcher
            blockDict = pileupDict[pileupType]

            for dataset in datasets:
                dbsFileBlocks = reader.listFileBlocks(dataset = dataset)
                for dbsFileBlockName in dbsFileBlocks:
                    fileList = [] # list of files in the block (dbsFile["LogicalFileName"])
                    storageElemNames = set() # list of StorageElementName
                    # each DBS block has a list under 'StorageElementList', iterate over
                    storageElements = reader.listFileBlockLocation(dbsFileBlockName)
                    for storElem in storageElements:
                        storageElemNames.add(storElem)
                    # now get list of files in the block
                    dbsFiles = reader.listFilesInBlock(dbsFileBlockName)
                    for dbsFile in dbsFiles:
                        fileList.append(dbsFile["LogicalFileName"])
                    # now compare the sets:
                    m = ("StorageElementNames don't agree for pileup type '%s', "
                         "dataset '%s' in configuration: '%s'" % (pileupType, dataset, pileupDict))
                    self.assertEqual(set(blockDict[dbsFileBlockName]["StorageElementNames"]), storageElemNames, m)
                    m = ("FileList don't agree for pileup type '%s', dataset '%s' "
                         " in configuration: '%s'" % (pileupType, dataset, pileupDict))
                    print fileList
                    print blockDict[dbsFileBlockName]["FileList"]
                    self.assertEqual(sorted(blockDict[dbsFileBlockName]["FileList"]), sorted(fileList))

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
                    pileupDict = decoder.decode(json)
                    f.close()
                except IOError:
                    m = "Could not read pileup JSON configuration file: '%s'" % pileupConfig
                    self.fail(m)
                self._queryAndCompareWithDBS(pileupDict, defaultArguments, helper.data.dbsUrl)

    def testPileupFetcherOnMC(self):
        pileupMcArgs = MonteCarloWorkloadFactory.getTestArguments()
        pileupMcArgs["PileupConfig"] = {"cosmics": ["/Mu/PenguinsPenguinsEverywhere-SingleMu-HorriblyJaundicedYellowEyedPenginsSearchingForCarrots-v31/RECO"],
                                        "minbias": ["/Mu/PenguinsPenguinsEverywhere-SingleMu-HorriblyJaundicedYellowEyedPenginsSearchingForCarrots-v31/RECO"]}
        pileupMcArgs["CouchURL"] = os.environ["COUCHURL"]
        pileupMcArgs["CouchDBName"] = "pileupfetcher_t"
        pileupMcArgs["ConfigCacheID"] = self.injectGenerationConfig()

        factory = MonteCarloWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", pileupMcArgs)

        # Since this is test of the fetcher - The loading from WMBS isn't
        # really necessary because the fetching happens before the workflow
        # is inserted into WMBS: feed the workload instance directly into fetcher:
        fetcher = PileupFetcher()
        creator = SandboxCreator()
        pathBase = "%s/%s" % (self.testDir, testWorkload.name())
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
                self._queryPileUpConfigFile(pileupMcArgs, task, taskPath)

if __name__ == "__main__":
    unittest.main()
