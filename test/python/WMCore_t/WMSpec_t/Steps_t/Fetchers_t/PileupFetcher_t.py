"""
_PileupFetcher_t_

The purpose of this test is to track what is happening in the PileupFetcher module.
The test instantiates a MC worklow and calls the PileupFetcher on its generation task.
"""
from __future__ import print_function

import os
import unittest
from json import JSONDecoder

import WMCore.WMSpec.WMStep as WMStep
import WMCore.WMSpec.WMTask as WMTask
from WMCore.Database.CMSCouch import CouchServer, Document
from WMCore.Services.DBS.DBS3Reader import DBS3Reader
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.WMRuntime.SandboxCreator import SandboxCreator
from WMCore.WMSpec.StdSpecs.TaskChain import TaskChainWorkloadFactory
from WMCore.WMSpec.Steps.Fetchers.PileupFetcher import PileupFetcher
from WMCore.WMSpec.WMWorkloadTools import parsePileupConfig
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase
from WMQuality.TestInitCouchApp import TestInitCouchApp


class PileupFetcherTest(EmulatedUnitTestCase):
    def setUp(self):
        """
        Initialize the database and couch.

        """
        super(PileupFetcherTest, self).setUp()

        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setupCouch("pileupfetcher_t", "ConfigCache")
        self.testInit.setSchema(customModules=["WMCore.WMBS"],
                                useDefault=False)
        couchServer = CouchServer(os.environ["COUCHURL"])
        self.configDatabase = couchServer.connectDatabase("pileupfetcher_t")
        self.testDir = self.testInit.generateWorkDir()

    def tearDown(self):
        """
        Clear out the database.
        """
        self.testInit.tearDownCouch()
        self.testInit.clearDatabase()
        self.testInit.delWorkDir()
        super(PileupFetcherTest, self).tearDown()  # Left here in case it's needed by any of the sub-classes

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
        reader = DBS3Reader(dbsUrl)
        # FIXME: this will have to be converted to Rucio data location, eventually...
        phedex = PhEDEx()

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
        #    {"pileupTypeA": {"BlockA": {"FileList": [], "PhEDExNodeNames": []},
        #                     "BlockB": {"FileList": [], "PhEDExNodeNames": []}, ....}
        for pileupType, datasets in inputArgs.items():
            # this is from the pileup configuration produced by PileupFetcher
            blockDict = pileupDict[pileupType]

            for dataset in datasets:
                dbsFileBlocks = reader.listFileBlocks(dataset=dataset)
                blocksLocation = phedex.getReplicaPhEDExNodesForBlocks(dataset=dataset, complete='y')
                for dbsFileBlockName in dbsFileBlocks:
                    fileList = []
                    pnns = set()
                    for pnn in blocksLocation[dbsFileBlockName]:
                        pnns.add(pnn)
                    # now get list of files in the block
                    dbsFiles = reader.listFilesInBlock(dbsFileBlockName)
                    for dbsFile in dbsFiles:
                        fileList.append(dbsFile["LogicalFileName"])
                    # now compare the sets:
                    m = ("PNNs don't agree for pileup type '%s', "
                         "dataset '%s' in configuration: '%s'" % (pileupType, dataset, pileupDict))
                    self.assertEqual(set(blockDict[dbsFileBlockName]["PhEDExNodeNames"]), pnns, m)
                    m = ("FileList don't agree for pileup type '%s', dataset '%s' "
                         " in configuration: '%s'" % (pileupType, dataset, pileupDict))
                    self.assertItemsEqual(blockDict[dbsFileBlockName]["FileList"], fileList, m)

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
                    with open(pileupConfig) as fObj:
                        pileupDict = decoder.decode(fObj.read())
                except IOError:
                    m = "Could not read pileup JSON configuration file: '%s'" % pileupConfig
                    self.fail(m)
                self._queryAndCompareWithDBS(pileupDict, defaultArguments, helper.data.dbsUrl)

    def testPileupFetcherOnMC(self):
        pileupMcArgs = TaskChainWorkloadFactory.getTestArguments()
        pileupMcArgs['Task1']["MCPileup"] = "/Cosmics/ComissioningHI-PromptReco-v1/RECO"
        pileupMcArgs['Task1']["DataPileup"] = "/HighPileUp/Run2011A-v1/RAW"
        pileupMcArgs['Task1']["ConfigCacheID"] = self.injectGenerationConfig()
        pileupMcArgs["CouchDBName"] = "pileupfetcher_t"
        pileupMcArgs["CouchURL"] = os.environ["COUCHURL"]

        factory = TaskChainWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", pileupMcArgs)

        # now that the workload was created and args validated, we can add this PileupConfig
        pileupMcArgs["PileupConfig"] = parsePileupConfig(pileupMcArgs['Task1']["MCPileup"],
                                                         pileupMcArgs['Task1']["DataPileup"])

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
