#!/usr/bin/env python
"""
_SetupCMSSWPset_t.py

Tests for the PSet configuration code.

"""

import imp
import unittest
import pickle
import os
import sys
import nose

from WMCore.DataStructs.File import File
from WMCore.DataStructs.Job import Job
from WMCore.Configuration import ConfigSection
from WMCore.Storage.TrivialFileCatalog import loadTFC

from WMCore.WMSpec.WMStep import WMStep
from WMCore.WMSpec.Steps.Templates.CMSSW import CMSSWStepHelper
from WMCore.WMSpec.Steps import StepFactory
from WMCore.WMSpec.Steps.Fetchers.PileupFetcher import PileupFetcher
from WMCore.Storage.SiteLocalConfig import loadSiteLocalConfig

from WMQuality.TestInit import TestInit
import WMCore.WMBase

class SetupCMSSWPsetTest(unittest.TestCase):
    def setUp(self):
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testDir = self.testInit.generateWorkDir()
        sys.path.insert(0, os.path.join(WMCore.WMBase.getTestBase(),
                                        "WMCore_t/WMRuntime_t/Scripts_t"))

    def tearDown(self):
        sys.path.remove(os.path.join(WMCore.WMBase.getTestBase(),
                                     "WMCore_t/WMRuntime_t/Scripts_t"))
        del sys.modules["WMTaskSpace"]
        self.testInit.delWorkDir()
        os.unsetenv("WMAGENT_SITE_CONFIG_OVERRIDE")

    def createTestStep(self):
        """
        _createTestStep_

        Create a test step that can be passed to the setup script.

        """
        newStep = WMStep("cmsRun1")
        newStepHelper = CMSSWStepHelper(newStep)
        newStepHelper.setStepType("CMSSW")
        newStepHelper.setGlobalTag("SomeGlobalTag")
        stepTemplate = StepFactory.getStepTemplate("CMSSW")
        stepTemplate(newStep)
        newStep.application.command.configuration = "PSet.py"
        return newStepHelper


    def createTestJob(self):
        """
        _createTestJob_

        Create a test job that has parents for each input file.

        """
        newJob = Job(name = "TestJob")
        newJob.addFile(File(lfn = "/some/file/one",
                            parents = set([File(lfn = "/some/parent/one")])))
        newJob.addFile(File(lfn = "/some/file/two",
                            parents = set([File(lfn = "/some/parent/two")])))
        return newJob


    def testPSetFixup(self):
        """
        _testPSetFixup_

        Verify that all necessary parameters are set in the PSet.

        """
        from WMCore.WMRuntime.Scripts.SetupCMSSWPset import SetupCMSSWPset
        setupScript = SetupCMSSWPset()
        setupScript.step = self.createTestStep()
        setupScript.stepSpace = ConfigSection(name = "stepSpace")
        setupScript.stepSpace.location = self.testDir
        setupScript.job = self.createTestJob()
        setupScript()

        testFile = os.path.join(self.testDir, "PSet.py")
        pset = imp.load_source('process', testFile)
        fixedPSet = pset.process

        self.assertEqual(len(fixedPSet.source.fileNames.value), 2,
                         "Error: Wrong number of files.")
        self.assertEqual(len(fixedPSet.source.secondaryFileNames.value), 2,
                         "Error: Wrong number of secondary files.")
        self.assertEqual(fixedPSet.source.fileNames.value[0], "/some/file/one",
                         "Error: Wrong input file.")
        self.assertEqual(fixedPSet.source.fileNames.value[1], "/some/file/two",
                         "Error: Wrong input file.")
        self.assertEqual(fixedPSet.source.secondaryFileNames.value[0], "/some/parent/one",
                         "Error: Wrong input file.")
        self.assertEqual(fixedPSet.source.secondaryFileNames.value[1], "/some/parent/two",
                         "Error: Wrong input file.")
        self.assertEqual(fixedPSet.maxEvents.input.value, -1,
                         "Error: Wrong maxEvents.")

    def testEventsPerLumi(self):
        """
        _testEventsPerLumi_
        Verify that you can put in events per lumi in the process.

        """
        from WMCore.WMRuntime.Scripts.SetupCMSSWPset import SetupCMSSWPset
        setupScript = SetupCMSSWPset()
        setupScript.step = self.createTestStep()
        setupScript.step.setEventsPerLumi(500)
        setupScript.stepSpace = ConfigSection(name = "stepSpace")
        setupScript.stepSpace.location = self.testDir
        setupScript.job = self.createTestJob()
        setupScript()

        testFile = os.path.join(self.testDir, "PSet.py")
        pset = imp.load_source('process', testFile)
        fixedPSet = pset.process

        self.assertEqual(len(fixedPSet.source.fileNames.value), 2,
                         "Error: Wrong number of files.")
        self.assertEqual(len(fixedPSet.source.secondaryFileNames.value), 2,
                         "Error: Wrong number of secondary files.")
        self.assertEqual(fixedPSet.source.fileNames.value[0], "/some/file/one",
                         "Error: Wrong input file.")
        self.assertEqual(fixedPSet.source.fileNames.value[1], "/some/file/two",
                         "Error: Wrong input file.")
        self.assertEqual(fixedPSet.source.secondaryFileNames.value[0], "/some/parent/one",
                         "Error: Wrong input file.")
        self.assertEqual(fixedPSet.source.secondaryFileNames.value[1], "/some/parent/two",
                         "Error: Wrong input file.")
        self.assertEqual(fixedPSet.source.numberEventsInLuminosityBlock.value,
                         500, "Error: Wrong number of events per luminosity block")
        self.assertEqual(fixedPSet.maxEvents.input.value, -1,
                         "Error: Wrong maxEvents.")

    def testChainedProcesing(self):
        """
        test for chained CMSSW processing - check the overriden TFC, its values
        and that input files is / are set correctly.

        """
        from WMCore.WMRuntime.Scripts.SetupCMSSWPset import SetupCMSSWPset
        setupScript = SetupCMSSWPset()
        setupScript.step = self.createTestStep()
        setupScript.stepSpace = ConfigSection(name = "stepSpace")
        setupScript.stepSpace.location = self.testDir
        setupScript.job = self.createTestJob()
        setupScript.step.setupChainedProcessing("my_first_step", "my_input_module")
        setupScript()

        # test if the overriden TFC is right
        self.assertTrue(hasattr(setupScript.step.data.application, "overrideCatalog"),
                        "Error: overriden TFC was not set")
        tfc = loadTFC(setupScript.step.data.application.overrideCatalog)
        inputFile = "../my_first_step/my_input_module.root"
        self.assertEqual(tfc.matchPFN("direct", inputFile), inputFile)
        self.assertEqual(tfc.matchLFN("direct", inputFile), inputFile)

        self.assertEqual(setupScript.process.source.fileNames.value, [inputFile])


    def testPileupSetup(self):
        """
        Test the pileup setting.

        reference (setupScript.process instance):
        in test/python/WMCore_t/WMRuntime_t/Scripts_t/WMTaskSpace/cmsRun1/PSet.py

        """
        try:
             from DBSAPI.dbsApi import DbsApi
        except ImportError, ex:
            raise nose.SkipTest

        # this is modified and shortened version of
        # WMCore/test/python/WMCore_t/Misc_t/site-local-config.xml
        # since the dataset name in question (below) is only present at
        # storm-fe-cms.cr.cnaf.infn.it, need to make the test think it's its local SE
        siteLocalConfigContent = \
        """
<site-local-config>
    <site name="-SOME-SITE-NAME-">
        <event-data>
            <catalog url="trivialcatalog_file:/uscmst1/prod/sw/cms/SITECONF/T1_US_FNAL/PhEDEx/storage.xml?protocol=dcap"/>
        </event-data>
        <local-stage-out>
            <!-- original cmssrm.fnal.gov -->
            <se-name value="storm-fe-cms.cr.cnaf.infn.it"/>
            <command value="test-copy"/>
            <catalog url="trivialcatalog_file:/uscmst1/prod/sw/cms/SITECONF/T1_US_FNAL/PhEDEx/storage.xml?protocol=dcap"/>
        </local-stage-out>
        <calib-data>
            <frontier-connect>
                <load balance="proxies"/>
                <proxy url="http://cmsfrontier1.fnal.gov:3128"/>
                <proxy url="http://cmsfrontier2.fnal.gov:3128"/>
            </frontier-connect>
        </calib-data>
    </site>
</site-local-config>
"""
        siteLocalConfig = os.path.join(self.testDir, "test-site-local-config.xml")
        f = open(siteLocalConfig, 'w')
        f.write(siteLocalConfigContent)
        f.close()

        from WMCore.WMRuntime.Scripts.SetupCMSSWPset import SetupCMSSWPset
        setupScript = SetupCMSSWPset()
        setupScript.step = self.createTestStep()
        setupScript.stepSpace = ConfigSection(name = "stepSpace")
        setupScript.stepSpace.location = os.path.join(self.testDir, "cmsRun1")
        setupScript.job = self.createTestJob()
        # define pileup configuration
        # despite of the implementation considering whichever type of pileup,
        # only "data" and "mc" types are eventually considered and lead to any
        # modifications of job input files
        pileupConfig = {"data": ["/Mu/PenguinsPenguinsEverywhere-SingleMu-HorriblyJaundicedYellowEyedPenginsSearchingForCarrots-v31/RECO"],
                        "mc": ["/Mu/PenguinsPenguinsEverywhere-SingleMu-HorriblyJaundicedYellowEyedPenginsSearchingForCarrots-v31/RECO"]}
        dbsUrl = "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
        setupScript.step.setupPileup(pileupConfig, dbsUrl)
        # SetupCMSSWPset pileup handling will be consulting SiteLocalConfig
        # to determine StorageElement (SE) name the job is running on
        # SiteLocalConfig loads the site-local-config.xml file from env.
        # variable defined location ; if the variable is not defined already, set it
        # obviously, if "WMAGENT_SITE_CONFIG_OVERRIDE" is already set here, the above
        # thick with SE name is not effective
        if not os.getenv("WMAGENT_SITE_CONFIG_OVERRIDE", None):
            os.environ["WMAGENT_SITE_CONFIG_OVERRIDE"] = siteLocalConfig
        # find out local site name from the testing local site config,
        # will be needed later
        siteConfig = loadSiteLocalConfig()
        seLocalName = siteConfig.localStageOut["se-name"]
        print "Running on site '%s', local SE name: '%s'" % (siteConfig.siteName, seLocalName)

        # before calling the script, SetupCMSSWPset will try to load JSON
        # pileup configuration file, need to create it in self.testDir
        fetcher = PileupFetcher()
        fetcher.setWorkingDirectory(self.testDir)
        fetcher._createPileupConfigFile(setupScript.step)

        setupScript()

        # now test all modifications carried out in SetupCMSSWPset.__call__
        # which will also test that CMSSWStepHelper.setupPileup run correctly
        mixModules, dataMixModules = setupScript._getPileupMixingModules()

        # load in the pileup configuration in the form of dict which
        # PileupFetcher previously saved in a JSON file
        pileupDict = setupScript._getPileupConfigFromJson()

        # get the sub dict for particular pileup type
        # for pileupDict structure description - see PileupFetcher._queryDbsAndGetPileupConfig
        for pileupType, modules in zip(("data", "mc"), (dataMixModules, mixModules)):
            # getting KeyError here - above pileupConfig is not correct - need
            # to have these two types of pile type
            d = pileupDict[pileupType]
            self._mixingModulesInputFilesTest(modules, d, seLocalName)


    def _mixingModulesInputFilesTest(self, modules, pileupSubDict, seLocalName):
        """
        pileupSubDic - contains only dictionary for particular pile up type

        """
        # consider only locally available files
        filesInConfigDict = []
        for v in pileupSubDict.values():
            if seLocalName in v["StorageElementNames"]:
                filesInConfigDict.extend(v["FileList"])

        for m in modules:
            inputTypeAttrib = getattr(m, "input", None) or getattr(m, "secsource", None)
            fileNames = inputTypeAttrib.fileNames.value
            if fileNames == None:
                fileNames = []
            m = ("Pileup configuration file list '%s' and mixing modules input "
                 "filelist '%s' are not identical." % (filesInConfigDict, fileNames))
            self.assertEqual(filesInConfigDict, fileNames, m)



if __name__ == "__main__":
    unittest.main()
