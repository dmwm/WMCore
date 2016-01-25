#!/usr/bin/env python
"""
_WMTask_t_

Unit tests for the WMTask class.
"""

import unittest

from WMCore.WMSpec.WMTask import WMTask, makeWMTask
from WMCore.DataStructs.LumiList import LumiList
from WMCore.WMSpec.WMStep import makeWMStep

class WMTaskTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testInstantiation(self):
        """
        _testInstantiation_

        Verify that the WMTask and the WMTaskHelper classes can be
        instantiated.
        """
        WMTask("task1")
        makeWMTask("task2")

        return

    def testTreeBuilding(self):
        """
        _testTreeBuilding_

        Verify that tasks can be created and arranged in a hierarchy.
        """
        task1 = makeWMTask("task1")
        task1.addTask("task2a")
        task1.addTask("task2b")
        task1.addTask("task2c")

        goldenTasks = ["task2a", "task2b", "task2c"]
        for childTask in task1.childTaskIterator():
            assert childTask.name() in goldenTasks, \
                   "Error: Unknown child task: %s" % childTask.name()

            goldenTasks.remove(childTask.name())

        assert len(goldenTasks) == 0, \
               "Error: Missing tasks."
        return

    def testAddingSteps(self):
        """
        _testAddingSteps_

        Verify that adding steps to a task works correctly.
        """
        task1 = makeWMTask("task1")

        task2a = task1.addTask("task2a")
        task2b = task1.addTask("task2b")
        task2c = task1.addTask("task2c")
        task2a.addTask("task3")

        step1 = makeWMStep("step1")
        step1.addStep("step1a")
        step1.addStep("step1b")
        step1.addStep("step1c")

        step2 = makeWMStep("step2")
        step2.addStep("step2a")
        step2.addStep("step2b")
        step2.addStep("step2c")

        step3 = makeWMStep("step3")
        step3.addStep("step3a")
        step3.addStep("step3b")
        step3.addStep("step3c")

        step4 = makeWMStep("step4")
        step4.addStep("step4a")
        step4.addStep("step4b")
        step4.addStep("step4c")

        task1.setStep(step1)
        task2a.setStep(step2)
        task2b.setStep(step3)
        task2c.setStep(step4)

        self.assertEqual(task1.getStep("step1a").name(), "step1a")
        self.assertEqual(task2a.getStep("step2b").name(), "step2b")
        self.assertEqual(task2b.getStep("step3c").name(), "step3c")
        self.assertEqual(task2c.getStep("step4a").name(), "step4a")

        self.assertEqual(task1.getStep("step2"), None)
        self.assertEqual(task2a.getStep("step4"), None)
        self.assertEqual(task2b.getStep("step2"), None)
        self.assertEqual(task2c.getStep("step1"), None)

        self.assertEqual(task1.listNodes(), ['task1', 'task2a', 'task3', 'task2b', 'task2c'])
        return

    def testSiteWhiteBlacklists(self):
        """
        _testSiteWhiteBlacklists_

        Verify that methods for setting and retrieving site white and black
        lists work correctly.
        """
        testTask = makeWMTask("TestTask")
        testTask.setSiteWhitelist(["T1_US_FNAL", "T1_DE_KIT"])
        testTask.setSiteBlacklist(["T1_IT_IN2P3", "T1_CH_CERN", "T2_US_NEBRASKA"])

        taskWhitelist = testTask.siteWhitelist()

        assert len(taskWhitelist) == 2, \
               "Error: Wrong number of sites in white list."
        assert "T1_US_FNAL" in taskWhitelist, \
               "Error: Site missing from white list."
        assert "T1_DE_KIT" in taskWhitelist, \
               "Error: Site missing from white list."

        taskBlacklist = testTask.siteBlacklist()

        assert len(taskBlacklist) == 3, \
               "Error: Wrong number if sites in black list."
        assert "T1_IT_IN2P3" in taskBlacklist, \
               "Error: Site missing from black list."
        assert "T1_CH_CERN" in taskBlacklist, \
               "Error: Site missing from black list."
        assert "T2_US_NEBRASKA" in taskBlacklist, \
               "Error: Site missing from black list."

        return

    def testJobSplittingArgs(self):
        """
        _testJobSplittingArgs_

        Verify that setting job splitting arguments for a task works correctly.
        When pulling the job splitting arguments out of a task the site white
        list and black list should also be included as they need to be passed to
        the job splitting code.
        """
        testTask = makeWMTask("TestTask")
        testTask.setTaskType("Processing")

        self.assertEqual(testTask.taskType(), "Processing",
                         "Error: Wrong task type.")

        testTask.setJobResourceInformation(timePerEvent = 12, memoryReq = 2300000,
                                           sizePerEvent = 512)
        testTask.setSplittingAlgorithm("MadeUpAlgo", events_per_job = 100,
                                       max_job_size = 24,
                                       one_more_param = "Hello")
        testTask.setSiteWhitelist(["T1_US_FNAL", "T1_CH_CERN"])
        testTask.setSiteBlacklist(["T2_US_PERDUE", "T2_US_UCSD", "T1_TW_ASGC"])

        testTask.addInputDataset(primary = "PrimaryDataset",
                                 processed = "ProcessedDataset",
                                 tier = "DataTier",
                                 dbsurl = "DBSURL",
                                 block_whitelist = ["Block1", "Block2"],
                                 block_blacklist = ["Block3", "Block4", "Block5"],
                                 run_whitelist = [1, 2, 3],
                                 run_blacklist = [4, 5])

        # Make sure we can set individual performance parameters without affecting the others
        testTask.setJobResourceInformation(timePerEvent = 14)

        self.assertEqual(testTask.jobSplittingAlgorithm(),"MadeUpAlgo",
               "Error: Wrong job splitting algorithm name.")

        algoParams = testTask.jobSplittingParameters(performance = False)
        self.assertEqual(len(algoParams), 9,
                         "Error: Wrong number of algo parameters.")
        algoParams = testTask.jobSplittingParameters()
        self.assertEqual(len(algoParams), 10,
                         "Error: Wrong number of algo parameters.")

        self.assertTrue("algorithm" in algoParams,
                        "Error: Missing algo parameter.")
        self.assertEqual(algoParams["algorithm"], "MadeUpAlgo",
                         "Error: Parameter has wrong value.")

        self.assertTrue("events_per_job" in algoParams,
                        "Error: Missing algo parameter.")
        self.assertEqual(algoParams["events_per_job"], 100,
                         "Error: Parameter has wrong value.")

        self.assertTrue("max_job_size" in algoParams,
                        "Error: Missing algo parameter.")
        self.assertEqual(algoParams["max_job_size"], 24,
                         "Error: Parameter has wrong value.")

        self.assertTrue("one_more_param" in algoParams,
                        "Error: Missing algo parameter.")
        self.assertEqual(algoParams["one_more_param"], "Hello",
                         "Error: Parameter has wrong value.")

        self.assertTrue("runWhitelist" in algoParams,
                        "Error: Missing algo parameter.")
        self.assertEqual(len(algoParams["runWhitelist"]), 3,
                         "Error: Wrong number of runs in whitelist.")

        self.assertTrue("performance" in algoParams,
                        "Error: Missing algo parameter.")
        self.assertEqual(algoParams["performance"]["timePerEvent"], 14,
                         "Error: Wrong time per event")
        self.assertEqual(algoParams["performance"]["memoryRequirement"], 2300000,
                         "Error: Wrong memory requirement")
        self.assertEqual(algoParams["performance"]["sizePerEvent"], 512,
                         "Error: Wrong size per event")

        return

    def testInputDataset(self):
        """
        _testInputDataset_

        Verify that the addInputDataset() method works correctly and that the
        run/block black and white lists can be changed after calling
        addInputDataset().
        """
        testTask = makeWMTask("TestTask")

        assert testTask.getInputDatasetPath() == None, \
               "Error: Input dataset path should be None."
        assert testTask.inputDatasetDBSURL() == None, \
               "Error: Input DBS URL should be None."
        assert testTask.inputBlockWhitelist() == None, \
               "Error: Input block white list should be None."
        assert testTask.inputBlockBlacklist() == None, \
               "Error: Input block black list should be None."
        assert testTask.inputRunWhitelist() == None, \
               "Error: Input run white list should be None."
        assert testTask.inputRunBlacklist() == None, \
               "Error: Input run black list should be None."

        testTask.addInputDataset(primary = "PrimaryDataset",
                                 processed = "ProcessedDataset",
                                 tier = "DataTier",
                                 dbsurl = "DBSURL",
                                 block_whitelist = ["Block1", "Block2"],
                                 block_blacklist = ["Block3", "Block4", "Block5"],
                                 run_whitelist = [1, 2, 3],
                                 run_blacklist = [4, 5])

        assert testTask.getInputDatasetPath() == "/PrimaryDataset/ProcessedDataset/DataTier", \
               "Error: Input dataset path is wrong."
        assert testTask.inputDatasetDBSURL() == "DBSURL", \
               "Error: Input DBS URL is wrong."

        assert len(testTask.inputBlockWhitelist()) == 2, \
               "Error: Wrong number of blocks in white list."
        assert "Block1" in testTask.inputBlockWhitelist(), \
               "Error: Block missing from white list."
        assert "Block2" in testTask.inputBlockWhitelist(), \
               "Error: Block missing from white list."

        assert len(testTask.inputBlockBlacklist()) == 3, \
               "Error: Wrong number of blocks in black list."
        assert "Block3" in testTask.inputBlockBlacklist(), \
               "Error: Block missing from black list."
        assert "Block4" in testTask.inputBlockBlacklist(), \
               "Error: Block missing from black list."
        assert "Block5" in testTask.inputBlockBlacklist(), \
               "Error: Block missing from black list."

        assert len(testTask.inputRunWhitelist()) == 3, \
               "Error: Wrong number of runs in white list."
        assert 1 in testTask.inputRunWhitelist(), \
               "Error: Run is missing from white list."
        assert 2 in testTask.inputRunWhitelist(), \
               "Error: Run is missing from white list."
        assert 3 in testTask.inputRunWhitelist(), \
               "Error: Run is missing from white list."

        assert len(testTask.inputRunBlacklist()) == 2, \
               "Error: Wrong number of runs in black list."
        assert 4 in testTask.inputRunBlacklist(), \
               "Error: Run is missing from black list."
        assert 5 in testTask.inputRunBlacklist(), \
               "Error: Run is missing from black list."

        testTask.setInputBlockWhitelist(["Block6"])

        assert len(testTask.inputBlockWhitelist()) == 1, \
               "Error: Wrong number of blocks in white list."
        assert "Block6" in testTask.inputBlockWhitelist(), \
               "Error: Block missing from white list."

        testTask.setInputBlockBlacklist(["Block7", "Block8"])

        assert len(testTask.inputBlockBlacklist()) == 2, \
               "Error: Wrong number of blocks in black list."
        assert "Block7" in testTask.inputBlockBlacklist(), \
               "Error: Block missing from black list."
        assert "Block8" in testTask.inputBlockBlacklist(), \
               "Error: Block missing from black list."

        testTask.setInputRunWhitelist([6])

        assert len(testTask.inputRunWhitelist()) == 1, \
               "Error: Wrong number of runs in white list."
        assert 6 in testTask.inputRunWhitelist(), \
               "Error: Run missing from white list."

        testTask.setInputRunBlacklist([7, 8])

        assert len(testTask.inputRunBlacklist()) == 2, \
               "Error: Wrong number of runs in black list."
        assert 7 in testTask.inputRunBlacklist(), \
               "Error: Run missing from black list."
        assert 8 in testTask.inputRunBlacklist(), \
               "Error: Run missing from black list."

        return

    def testAddNotifications(self):
        """
        _testAddNotifications_

        Test whether we can add notification addresses
        """

        testTask = makeWMTask("TestTask")
        testTask.setTaskType("Processing")

        targetList = ['loser@fnal.gov', 'loser@cern.ch']

        self.assertEqual(testTask.getNotifications(), [])

        for x in targetList:
            testTask.addNotification(target = x)

        self.assertEqual(testTask.getNotifications(), targetList)

    def testPerformanceMonitor(self):
        """
        _testPerformanceMonitor_

        Test automated adding of performanceMonitor
        Really you shouldn't be using this, so don't add it.
        """

        testTask = makeWMTask("TestTask")

        testTask.setPerformanceMonitor(maxRSS = 100, maxVSize = 101, softTimeout = 100,
                                       gracePeriod = 1)

        self.assertEqual(testTask.data.watchdog.monitors, ['PerformanceMonitor'])
        self.assertEqual(testTask.data.watchdog.PerformanceMonitor.maxRSS, 100)
        self.assertEqual(testTask.data.watchdog.PerformanceMonitor.maxVSize, 101)
        self.assertEqual(testTask.data.watchdog.PerformanceMonitor.softTimeout, 100)
        self.assertEqual(testTask.data.watchdog.PerformanceMonitor.hardTimeout, 101)
        return

    def testProcessedDatasetElements(self):
        """
        _testProcessedDatasetElements_

        Test that we can add a processing version and acquisition era,
        and then get it back.
        """

        testTask = makeWMTask("TestTask")
        testTask.setAcquisitionEra("StoneAge")
        testTask.setProcessingVersion(2)
        testTask.setProcessingString("Test")

        self.assertEqual(testTask.getAcquisitionEra(), "StoneAge",
                         "Wrong acquisition era in the task")
        self.assertEqual(testTask.getProcessingVersion(), 2,
                         "Wrong processing version in the task")
        self.assertEqual(testTask.getProcessingString(), "Test",
                         "Wrong processing string in the task")

        return

    def testParameters(self):
        """
        _testParameters_

        Test any random junk that we throw into parameters
        """

        testTask = makeWMTask("TestTask")

        # Test the primarySubType first

        # Before we set anything, the subType should be none
        self.assertEqual(testTask.getPrimarySubType(), None)

        # After we set the task Type, but before the subType, it
        # should be the taskType
        testTask.setTaskType("SillyTask")
        self.assertEqual(testTask.getPrimarySubType(), "SillyTask")

        testTask.setPrimarySubType(subType = "subType")
        self.assertEqual(testTask.getPrimarySubType(), "subType")
        return

    def testBuildLumiMask(self):
        from WMCore.WMSpec.WMTask import buildLumiMask
        runs=['3','4']
        lumis=['1,4,23,45', '5,84,234,445']
        expected = {'3':[[1,4],[23,45]],'4':[[5,84],[234,445]]}

        #working
        self.assertEqual(buildLumiMask(runs, lumis), expected, "buildLumiMask")

        #number of runs different than number of lumis
        runs=['3']
        lumis=['1,4,23,45', '5,84,234,445']
        self.assertRaises(ValueError, buildLumiMask, runs, lumis)

        #wrong format of the number of lumis
        runs=['3', '4']
        lumis=['1,4,23,45', '5,84,234']
        self.assertRaises(ValueError, buildLumiMask, runs, lumis)

    def testAddLumiMask(self):
        """
        _testAddLumiMask_

        Verify that setting and getting the lumiMask objects for a task works correctly.
        Do a round trip of a typical lumi mask
        """
        testTask = makeWMTask("TestTask")

        lumiMask = LumiList(compactList = {
                '1': [[1, 33], [35, 35], [37, 47], [49, 75], [77, 130], [133, 136]],
                '2':[[1,45]],
                '3':[[1,45],[50,80]],
            })

        testTask.setLumiMask(lumiMask = lumiMask.getCompactList())
        outMask =  LumiList(compactList = testTask.getLumiMask())
        self.assertEqual(lumiMask.getCMSSWString(), outMask.getCMSSWString())

        return


    def testSubscriptionInformation(self):
        """
        _testSubscriptionInformation_

        Check the three methods related to the subscription information in a task
        Make sure that we can set the subscription information for all datasets produced by this task
        and we can select only some primaryDatasets/DataTiers, and check that we can update the dataset
        in a subscription information section.
        """
        testTask = makeWMTask("TestTask")
        cmsswStep = testTask.makeStep("cmsRun1")
        cmsswStep.setStepType("CMSSW")
        testTask.applyTemplates()
        cmsswHelper = cmsswStep.getTypeHelper()
        cmsswHelper.addOutputModule("outputRECO", primaryDataset = "OneParticle",
                                    processedDataset = "DawnOfAnEra-v1", dataTier = "RECO")
        cmsswHelper.addOutputModule("outputDQM", primaryDataset = "TwoParticles",
                                    processedDataset = "DawnOfAnEra-v1", dataTier = "DQM")
        cmsswHelper.addOutputModule("outputAOD", primaryDataset = "OneParticle",
                                    processedDataset = "DawnOfAnEra-v1", dataTier = "AOD")

        self.assertEqual(testTask.getSubscriptionInformation(), {}, "There should not be any subscription info")

        testTask.setSubscriptionInformation(custodialSites = ["mercury"],
                                            nonCustodialSites = ["mars", "earth"],
                                            autoApproveSites = ["earth"],
                                            custodialSubType = "Replica",
                                            nonCustodialSubType = "Move",
                                            custodialGroup = "DataOps",
                                            nonCustodialGroup = "AnalysisOps",
                                            priority = "Normal",
                                            deleteFromSource = True,
                                            primaryDataset = "OneParticle")
        subInfo = testTask.getSubscriptionInformation()

        outputRecoSubInfo = {"CustodialSites" : ["mercury"],
                             "NonCustodialSites" : ["mars", "earth"],
                             "AutoApproveSites" : ["earth"],
                             "CustodialSubType" : "Replica",
                             "NonCustodialSubType" : "Move",
                             "CustodialGroup" : "DataOps",
                             "NonCustodialGroup" : "AnalysisOps",
                             "Priority" : "Normal",
                             "DeleteFromSource" : True}

        self.assertEqual(subInfo["/OneParticle/DawnOfAnEra-v1/RECO"],
                         outputRecoSubInfo, "The RECO subscription information is wrong")
        self.assertTrue("/OneParticle/DawnOfAnEra-v1/AOD" in subInfo, "The AOD subscription information is wrong")
        self.assertFalse("/TwoParticles/DawnOfAnEra-v1/DQM" in subInfo, "The DQM subscription information is wrong")

        testTask.setSubscriptionInformation(custodialSites = ["jupiter"],
                                            primaryDataset = "TwoParticles")

        subInfo = testTask.getSubscriptionInformation()

        self.assertEqual(subInfo["/OneParticle/DawnOfAnEra-v1/RECO"],
                         outputRecoSubInfo, "The RECO subscription information is wrong")
        self.assertTrue("/OneParticle/DawnOfAnEra-v1/AOD" in subInfo, "The AOD subscription information is wrong")
        self.assertTrue("/TwoParticles/DawnOfAnEra-v1/DQM" in subInfo, "The DQM subscription information is wrong")

        recoutOutputModule = cmsswHelper.getOutputModule("outputRECO")
        setattr(recoutOutputModule, "primaryDataset", "ThreeParticles")

        testTask.updateSubscriptionDataset("outputRECO", recoutOutputModule)

        subInfo = testTask.getSubscriptionInformation()
        self.assertEqual(subInfo["/ThreeParticles/DawnOfAnEra-v1/RECO"],
                        outputRecoSubInfo, "The RECO subscription information is wrong")
        self.assertFalse("/OneParticle/DawnOfAnEra-v1/RECO" in subInfo, "The RECO subscription information is wrong")

    def testDeleteChild(self):
        """
        _testDeleteChild_

        Test that we can remove all reference from a child
        and other children are left intact
        """

        task1 = makeWMTask("task1")

        task1.addTask("task2a")
        task1.addTask("task2b")
        task1.addTask("task2c")
        task1.deleteChild("task2a")

        childrenNumber = 0
        for childTask in task1.childTaskIterator():
            if childTask.name() == "task2a":
                self.fail("Error: It was possible to find the deleted child")
            childrenNumber += 1
        self.assertEqual(childrenNumber, 2, "Error: Wrong number of children tasks")

        return

if __name__ == '__main__':
    unittest.main()
