#!/usr/bin/env python
"""
_LuminosityBased_t_

Luminosity based splitting test.
"""

import unittest
import os
import json

from WMCore.WMBase            import getTestBase

from WMCore.DataStructs.File import File
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.Subscription import Subscription
from WMCore.DataStructs.Workflow import Workflow
from WMCore.DataStructs.Run import Run

from WMCore.JobSplitting.SplitterFactory import SplitterFactory
from WMCore.Services.UUID import makeUUID

class LuminosityBasedTest(unittest.TestCase):
    """
    _LuminosityBasedTest_

    Test event based job splitting.
    """


    def setUp(self):
        """
        _setUp_

        Create two subscriptions: One that contains a single file and one that
        contains multiple files.
        """
        self.multipleFileFileset = Fileset(name = "TestFileset1")
        for i in range(10):
            newFile = File(makeUUID(), size = 20000, events = 2000)
            # Files will have a range of 80 lumi-sections each
            newFile.addRun(Run(207214, *range(80*i, 80*(i+1))))
            newFile.setLocation('se01')
       
            
            self.multipleFileFileset.addFile(newFile)

        self.singleFileFileset = Fileset(name = "TestFileset2")
        newFile = File("/some/file/name", size = 20000, events = 2000)
        newFile.setLocation('se02')
        newFile.addRun(Run(207214, *range(1, 80)))
        self.singleFileFileset.addFile(newFile)

        self.emptyFileFileset = Fileset(name = "TestFileset3")
        newFile = File("/some/file/name", size = 1000, events = 0)
        newFile.addRun(Run(207214, *range(1, 80)))
        newFile.setdefault('se03')
        self.emptyFileFileset.addFile(newFile)

        testWorkflow = Workflow()
        self.multipleFileSubscription = Subscription(fileset = self.multipleFileFileset,
                                                     workflow = testWorkflow,
                                                     split_algo = "LuminosityBased",
                                                     type = "Processing")
        self.singleFileSubscription = Subscription(fileset = self.singleFileFileset,
                                                   workflow = testWorkflow,
                                                   split_algo = "LuminosityBased",
                                                   type = "Processing")
        self.emptyFileSubscription = Subscription(fileset = self.emptyFileFileset,
                                                  workflow = testWorkflow,
                                                  split_algo = "LuminosityBased",
                                                  type = "Processing")

        self.performanceParams = {'timePerEvent' : 15,
                                  'memoryRequirement' : 1700,
                                  'sizePerEvent' : 400}
        # Simulate DQM input
        self.DQMLuminosityPerLs = self.loadTestDQMLuminosityPerLs()

        # Simulate DashBoard input
        self.testPerfCurve  =   self.loadTestPerfCurve()
        
        return

    def tearDown(self):
        """
        _tearDown_

        Nothing to do...
        """
        pass

    def loadTestDQMLuminosityPerLs(self):
        testResponseFile = open(os.path.join(getTestBase(),
        							'WMCore_t/JobSplitting_t/FakeInputs/DQMLuminosityPerLs.json'), 'r')
        response = testResponseFile.read()
        testResponseFile.close()
        responseJSON = json.loads(response)
        luminosityPerLS = responseJSON["hist"]["bins"]["content"]
        return luminosityPerLS 

    def loadTestPerfCurve(self):
        testResponseFile = open(os.path.join(getTestBase(),
        							'WMCore_t/JobSplitting_t/FakeInputs/dashBoardPerfCurve.json'), 'r')
        response = testResponseFile.read()
        testResponseFile.close()
        responseJSON = json.loads(response)
        perfCurve = responseJSON["points"][0]["data"]
        return perfCurve

		
    def testNoEvents(self):
        """
        _testNoEvents_

        Test luminosity based job splitting where there are no events in the
        input file, make sure the mask events are None
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.emptyFileSubscription)

        jobGroups = jobFactory(targetJobLength = 10800,
                               cmsswversion = "CMSSW_5_3_6",
                               performance = self.performanceParams,
                               testDqmLuminosityPerLs = self.DQMLuminosityPerLs,
                               testPerfCurve = self.testPerfCurve, 
                               primaryDataset = "SingleMu")
        
        self.assertEqual(len(jobGroups), 1,
                         "ERROR: JobFactory didn't return one JobGroup")
        self.assertEqual(len(jobGroups[0].jobs), 1,
                         "ERROR: JobFactory didn't create a single job")

        job = jobGroups[0].jobs.pop()

        self.assertEqual(job.getFiles(type = "lfn"), ["/some/file/name"],
                         "ERROR: Job contains unknown files")
        self.assertEqual(job["mask"].getMaxEvents(), None,
                         "ERROR: Mask maxEvents is not None")


        
    def testTooLongJobSplit(self):
        """
        _testTooLongJobSplit_

        Test luminosity based job splitting when we stretch a bit the limits - 70k seconds per job 
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.singleFileSubscription)
        

        jobGroups = jobFactory(targetJobLength = 70000,
                               cmsswversion = "CMSSW_5_3_6",
                               performance = self.performanceParams,
                               testDqmLuminosityPerLs = self.DQMLuminosityPerLs,
                               testPerfCurve = self.testPerfCurve, 
                               primaryDataset = "SingleMu")

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 1, \
               "ERROR: JobFactory created %s jobs not one" % len(jobGroups[0].jobs)

        firstEvents = []
        for job in jobGroups[0].jobs:

            assert job.getFiles(type = "lfn") == ["/some/file/name"], \
                   "ERROR: Job contains unknown files."

            assert job["mask"]["FirstEvent"] not in [-1, 1700], \
                   "ERROR: Job's first event is incorrect."

            assert job["mask"]["FirstEvent"] not in firstEvents, \
                   "ERROR: Job's first event is repeated."
            firstEvents.append(job["mask"]["FirstEvent"])

        return
        
    def test5mJobSplit(self):
        """
        _test5mJobSplit_

        Test luminosity based job splitting when we want 5m jobs.
        Stretching limits once more.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.singleFileSubscription)
        

        jobGroups = jobFactory(targetJobLength = 300,
                               cmsswversion = "CMSSW_5_3_6",
                               performance = self.performanceParams,
                               testDqmLuminosityPerLs = self.DQMLuminosityPerLs,
                               testPerfCurve = self.testPerfCurve, 
                               primaryDataset = "SingleMu")

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 182, \
               "ERROR: JobFactory created %s jobs not 182" % len(jobGroups[0].jobs)

        firstEvents = []
        for job in jobGroups[0].jobs:
            assert job.getFiles(type = "lfn") == ["/some/file/name"], \
                   "ERROR: Job contains unknown files."

            assert job["mask"]["FirstEvent"] not in [-1, 1700], \
                   "ERROR: Job's first event is incorrect."

            assert job["mask"]["FirstEvent"] not in firstEvents, \
                   "ERROR: Job's first event is repeated."
            firstEvents.append(job["mask"]["FirstEvent"])

        return


    def test1hJobSplit(self):
        """
        _test1hJobSplit_

        Test luminosity based job splitting when we want 1h jobs.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.singleFileSubscription)
        

        jobGroups = jobFactory(targetJobLength = 3600,
                               cmsswversion = "CMSSW_5_3_6",
                               performance = self.performanceParams,
                               testDqmLuminosityPerLs = self.DQMLuminosityPerLs,
                               testPerfCurve = self.testPerfCurve, 
                               primaryDataset = "SingleMu")

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 15, \
               "ERROR: JobFactory created %s jobs not fifteen" % len(jobGroups[0].jobs)

        firstEvents = []
        for job in jobGroups[0].jobs:
            assert job.getFiles(type = "lfn") == ["/some/file/name"], \
                   "ERROR: Job contains unknown files."

            assert job["mask"]["FirstEvent"] not in [-1, 1700], \
                   "ERROR: Job's first event is incorrect."

            assert job["mask"]["FirstEvent"] not in firstEvents, \
                   "ERROR: Job's first event is repeated."
            firstEvents.append(job["mask"]["FirstEvent"])

        return


    def test3hJobSplit(self):
        """
        _test3hJobSplit_

        Test luminosity based job splitting when we want 3h jobs.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.singleFileSubscription)
        

        jobGroups = jobFactory(targetJobLength = 10800,
                               cmsswversion = "CMSSW_5_3_6",
                               performance = self.performanceParams,
                               testDqmLuminosityPerLs = self.DQMLuminosityPerLs,
                               testPerfCurve = self.testPerfCurve, 
                               primaryDataset = "SingleMu")

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 5, \
               "ERROR: JobFactory created %s jobs not five" % len(jobGroups[0].jobs)

        firstEvents = []
        for job in jobGroups[0].jobs:
            assert job.getFiles(type = "lfn") == ["/some/file/name"], \
                   "ERROR: Job contains unknown files."

            assert job["mask"]["FirstEvent"] not in [-1, 1700], \
                   "ERROR: Job's first event is incorrect."

            assert job["mask"]["FirstEvent"] not in firstEvents, \
                   "ERROR: Job's first event is repeated."
            firstEvents.append(job["mask"]["FirstEvent"])

        return

    def test6hJobMultipleFileSplit(self):
        """
        _test6hJobMultipleFileSplit_

        Test luminosity based job splitting when we want 6h jobs. Multiple files fileset.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.multipleFileSubscription)

        jobGroups = jobFactory(targetJobLength = 21600,
                               cmsswversion = "CMSSW_5_3_6",
                               performance = self.performanceParams,
                               testDqmLuminosityPerLs = self.DQMLuminosityPerLs,
                               testPerfCurve = self.testPerfCurve, 
                               primaryDataset = "SingleMu")

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 23, \
               "ERROR: JobFactory created %s jobs not 23" % len(jobGroups[0].jobs)

        for job in jobGroups[0].jobs:
            assert len(job.getFiles(type = "lfn")) == 1, \
                   "ERROR: Job contains too many files."

        return

    def test5mJobMultipleFileSplit(self):
        """
        _test5mJobMultipleFileSplit_

        Test luminosity based job splitting when we want 5m jobs. Multiple files fileset.
        Non realistic scenario. Stretching limits.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.multipleFileSubscription)

        jobGroups = jobFactory(targetJobLength = 300,
                               cmsswversion = "CMSSW_5_3_6",
                               performance = self.performanceParams,
                               testDqmLuminosityPerLs = self.DQMLuminosityPerLs,
                               testPerfCurve = self.testPerfCurve, 
                               primaryDataset = "SingleMu")
        
        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 1273, \
               "ERROR: JobFactory created %s jobs not 1273" % len(jobGroups[0].jobs)

        for job in jobGroups[0].jobs:
            assert len(job.getFiles(type = "lfn")) == 1, \
                   "ERROR: Job contains too many files."
        return

    def test6hJobMultipleFileSplitNoPerfCurve(self):
        """
        _test6hJobMultipleFileSplitNoPerfCurve_

        Here we do the same as before, but we don't pass the performance curve.
        The expected behavior is that for 2000 events file, 1 job get 1440 and
        the other 660, as the default TpE is 15 seconds.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.multipleFileSubscription)

        # Make sure that the function will not try to make HTTP requests within the test:
        os.environ['X509_USER_PROXY'] = ""

        jobGroups = jobFactory(targetJobLength = 21600,
                               cmsswversion = "CMSSW_5_3_6",
                               performance = self.performanceParams,
                               testDqmLuminosityPerLs = self.DQMLuminosityPerLs,
                               primaryDataset = "SingleMu")

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 20, \
               "ERROR: JobFactory created %s jobs not 20" % len(jobGroups[0].jobs)

        for job in jobGroups[0].jobs:
            assert len(job.getFiles(type = "lfn")) == 1, \
                   "ERROR: Job contains too many files."
        return

    def test6hJobEventMultipleFileSplitNoDQMCurve(self):
        """
        _test6hJobMultipleFileSplitNoDQMCurve_

        Here we do the same as before, but we don't pass the DQM Curve.
        More to test how the code is going to handle it. Some improvements came
        from this test.
        The expected behavior is that for 2000 events file, 1 job get 1440 and
        the other 660, as the default TpE is 15 seconds.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.multipleFileSubscription)
        
        # Make sure that the function will not try to make HTTP requests within the test:
        os.environ['X509_USER_PROXY'] = ""

        jobGroups = jobFactory(targetJobLength = 21600,
                               cmsswversion = "CMSSW_5_3_6",
                               performance = self.performanceParams,
                               testPerfCurve = self.testPerfCurve, 
                               primaryDataset = "SingleMu")

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 20, \
               "ERROR: JobFactory created %s jobs not 20" % len(jobGroups[0].jobs)

        for job in jobGroups[0].jobs:
            assert len(job.getFiles(type = "lfn")) == 1, \
                   "ERROR: Job contains too many files."
        return

    def test6hJobMultipleFileSplitNoDQMCurveNoPerfCurve(self):
        """
        _test6hJobMultipleFileSplitNoDQMCurve_

        Here we do the same as before, but we don't pass the performance curve.
        The expected behavior is that for 2000 events file, 1 job get 1440 and
        the other 660, as the default TpE is 15 seconds.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.multipleFileSubscription)

        # Make sure that the function will not try to make HTTP requests within the test:
        os.environ['X509_USER_PROXY'] = ""

        jobGroups = jobFactory(targetJobLength = 21600,
                               cmsswversion = "CMSSW_5_3_6",
                               performance = self.performanceParams,
                               primaryDataset = "SingleMu")

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 20, \
               "ERROR: JobFactory created %s jobs not 20" % len(jobGroups[0].jobs)

        for job in jobGroups[0].jobs:
            assert len(job.getFiles(type = "lfn")) == 1, \
                   "ERROR: Job contains too many files."
        return

        

if __name__ == '__main__':
    unittest.main()
