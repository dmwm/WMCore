#!/usr/bin/env python
"""
_LumiBased_t

Test lumi based splitting.
"""

from __future__ import division

from builtins import range
import threading
import unittest
import random

from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.DataStructs.Run import Run
from WMCore.DAOFactory import DAOFactory
from WMCore.JobSplitting.SplitterFactory import SplitterFactory
from WMCore.Services.UUIDLib import makeUUID
from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit
from WMCore.ACDC.DataCollectionService import DataCollectionService
from WMCore.WMSpec.WMWorkload import newWorkload


def getJob(workload):
    """
    getJob

    Given a workload, get a job from it
    """
    job = Job()
    job["task"] = workload.getTask("reco").getPathName()
    job["workflow"] = workload.name()
    job["location"] = "T1_US_FNAL"
    job["owner"] = "evansde77"
    job["group"] = "DMWM"
    return job


class LumiBasedTest(unittest.TestCase):
    """
    _LumiBasedTest_

    Test job splitting using LumiBased
    """

    def setUp(self):
        """
        _setUp_

        Create two subscriptions: One that contains a single file and one that
        contains multiple files.
        """

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        # self.testInit.clearDatabase(modules = ['WMCore.WMBS'])
        self.testInit.setSchema(customModules=["WMCore.WMBS"], useDefault=False)
        self.testInit.setupCouch("lumi_t", "GroupUser", "ACDC")

        myThread = threading.currentThread()
        daofactory = DAOFactory(package="WMCore.WMBS", logger=myThread.logger, dbinterface=myThread.dbi)

        locationAction = daofactory(classname = "Locations.New")
        locationAction.execute(siteName='s1', pnn="T1_US_FNAL_Disk")
        locationAction.execute(siteName='s2', pnn="T2_CH_CERN")

        self.testWorkflow = Workflow(spec="spec.xml", owner="mnorman", name="wf001", task="Test")
        self.testWorkflow.create()

        self.performanceParams = {'timePerEvent': 12,
                                  'memoryRequirement': 2300,
                                  'sizePerEvent': 400}

        return

    def tearDown(self):
        """
        _tearDown_

        """
        self.testInit.clearDatabase()
        self.testInit.tearDownCouch()
        return

    def createSubscription(self, nFiles, lumisPerFile, twoSites=False, rand=False):
        """
        _createSubscription_

        Create a subscription for testing
        """

        baseName = makeUUID()

        testFileset = Fileset(name=baseName)
        testFileset.create()
        parentFile = File('%s_parent' % baseName, size=1000, events=100,
                          locations=set(["T1_US_FNAL_Disk"]))
        parentFile.create()
        for i in range(nFiles):
            newFile = File(lfn='%s_%i' % (baseName, i), size=1000,
                           events=100, locations="T1_US_FNAL_Disk")
            lumis = []
            for lumi in range(lumisPerFile):
                if rand:
                    lumis.append(random.randint(1000 * i, 1000 * (i + 1)))
                else:
                    lumis.append((100 * i) + lumi)
            newFile.addRun(Run(i, *lumis))
            newFile.create()
            newFile.addParent(parentFile['lfn'])
            testFileset.addFile(newFile)
        if twoSites:
            for i in range(nFiles):
                newFile = File(lfn='%s_%i_2' % (baseName, i), size=1000,
                               events=100, locations="T2_CH_CERN")
                lumis = []
                for lumi in range(lumisPerFile):
                    if rand:
                        lumis.append(random.randint(1000 * i, 1000 * (i + 1)))
                    else:
                        lumis.append((100 * i) + lumi)
                newFile.addRun(Run(i, *lumis))
                newFile.create()
                newFile.addParent(parentFile['lfn'])
                testFileset.addFile(newFile)
        testFileset.commit()

        testSubscription = Subscription(fileset=testFileset, workflow=self.testWorkflow,
                                        split_algo="LumiBased", type="Processing")
        testSubscription.create()

        return testSubscription

    def testA_FileSplitting(self):
        """
        _FileSplitting_

        Test that things work if we split files between jobs
        """
        splitter = SplitterFactory()

        oneSetSubscription = self.createSubscription(nFiles=10, lumisPerFile=1)
        jobFactory = splitter(package="WMCore.WMBS", subscription=oneSetSubscription)

        jobGroups = jobFactory(lumis_per_job=3, halt_job_on_file_boundaries=True, performance=self.performanceParams)
        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 10)
        for job in jobGroups[0].jobs:
            self.assertTrue(len(job['input_files']), 1)
            self.assertEqual(job['estimatedJobTime'], 100 * 12)
            self.assertEqual(job['estimatedDiskUsage'], 100 * 400)
            self.assertEqual(job['estimatedMemoryUsage'], 2300)

        twoLumiFiles = self.createSubscription(nFiles=5, lumisPerFile=2)
        jobFactory = splitter(package="WMCore.WMBS", subscription=twoLumiFiles)
        jobGroups = jobFactory(lumis_per_job=1, halt_job_on_file_boundaries=True, performance=self.performanceParams)
        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 10)
        for job in jobGroups[0].jobs:
            self.assertEqual(len(job['input_files']), 1)
            self.assertEqual(job['estimatedJobTime'], 50 * 12)
            self.assertEqual(job['estimatedDiskUsage'], 50 * 400)
            self.assertEqual(job['estimatedMemoryUsage'], 2300)

        wholeLumiFiles = self.createSubscription(nFiles=5, lumisPerFile=3)
        jobFactory = splitter(package="WMCore.WMBS", subscription=wholeLumiFiles)
        jobGroups = jobFactory(lumis_per_job=2, halt_job_on_file_boundaries=True, performance=self.performanceParams)
        self.assertEqual(len(jobGroups), 1)
        # 10 because we split on run boundaries
        self.assertEqual(len(jobGroups[0].jobs), 10)
        jobList = jobGroups[0].jobs
        for idx, job in enumerate(jobList, start=1):
            # Have should have one file, half two
            self.assertEqual(len(job['input_files']), 1)
            if idx % 2 == 0:
                self.assertEqual(job['estimatedJobTime'], (1.0 * round(100 / 3)) * 12)
                self.assertEqual(job['estimatedDiskUsage'], (1.0 * round(100 / 3)) * 400)
            else:
                self.assertEqual(job['estimatedJobTime'], (2.0 * round(100 / 3)) * 12)
                self.assertEqual(job['estimatedDiskUsage'], (2.0 * round(100 / 3)) * 400)
            self.assertEqual(job['estimatedMemoryUsage'], 2300)

        mask0 = jobList[0]['mask'].getRunAndLumis()
        self.assertEqual(mask0, {0: [[0, 1]]})
        mask1 = jobList[1]['mask'].getRunAndLumis()
        self.assertEqual(mask1, {0: [[2, 2]]})
        mask2 = jobList[2]['mask'].getRunAndLumis()
        self.assertEqual(mask2, {1: [[100, 101]]})
        mask3 = jobList[3]['mask'].getRunAndLumis()
        self.assertEqual(mask3, {1: [[102, 102]]})

        j0 = Job(id=jobList[0]['id'])
        j0.loadData()
        self.assertEqual(j0['mask'].getRunAndLumis(), {0: [[0, 1]]})

        # Do it with multiple sites
        twoSiteSubscription = self.createSubscription(nFiles=5, lumisPerFile=2, twoSites=True)
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=twoSiteSubscription)
        jobGroups = jobFactory(lumis_per_job=1,
                               halt_job_on_file_boundaries=True,
                               performance=self.performanceParams)
        self.assertEqual(len(jobGroups), 2)
        self.assertEqual(len(jobGroups[0].jobs), 10)
        for job in jobGroups[0].jobs:
            self.assertEqual(len(job['input_files']), 1)
            self.assertEqual(job['estimatedJobTime'], 50 * 12)
            self.assertEqual(job['estimatedDiskUsage'], 50 * 400)
            self.assertEqual(job['estimatedMemoryUsage'], 2300)

    def testB_NoRunNoFileSplitting(self):
        """
        _NoRunNoFileSplitting_

        Test the splitting algorithm in the odder fringe
        cases that might be required.
        """
        splitter = SplitterFactory()
        testSubscription = self.createSubscription(nFiles=5, lumisPerFile=5, twoSites=False)
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=testSubscription)

        jobGroups = jobFactory(lumis_per_job=3,
                               halt_job_on_file_boundaries=False,
                               splitOnRun=False,
                               performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 9)

        # The first job should have three lumis from one run
        # The second three lumis from two different runs
        self.assertEqual(jobs[0]['mask'].getRunAndLumis(), {0: [[0, 2]]})
        self.assertEqual(jobs[0]['estimatedJobTime'], 60 * 12)
        self.assertEqual(jobs[0]['estimatedDiskUsage'], 60 * 400)
        self.assertEqual(jobs[0]['estimatedMemoryUsage'], 2300)
        job1runLumi = jobs[1]['mask'].getRunAndLumis()
        self.assertEqual(job1runLumi[0][0][0] + 1, job1runLumi[0][0][1])  # Run 0, startLumi+1 == endLumi
        self.assertEqual(job1runLumi[1][0][0], job1runLumi[1][0][1])  # Run 1, startLumi == endLumi
        self.assertEqual(jobs[1]['estimatedJobTime'], 60 * 12)
        self.assertEqual(jobs[1]['estimatedDiskUsage'], 60 * 400)
        self.assertEqual(jobs[1]['estimatedMemoryUsage'], 2300)


        # And it should still be the same when you load it out of the database
        j1 = Job(id=jobs[1]['id'])
        j1.loadData()
        self.assertEqual(j1['mask'].getRunAndLumis(), {0: [[3, 4]], 1: [[100, 100]]})

        # Assert that this works differently with file splitting on and run splitting on
        testSubscription = self.createSubscription(nFiles=5, lumisPerFile=5, twoSites=False)
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=testSubscription)
        jobGroups = jobFactory(lumis_per_job=3,
                               halt_job_on_file_boundaries=True,
                               splitOnRun=True,
                               performance=self.performanceParams)
        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 10)

        # In this case it should slice things up so that each job only has one run
        # in it.
        self.assertEqual(jobs[0]['mask'].getRunAndLumis(), {0: [[0, 2]]})
        self.assertEqual(jobs[1]['mask'].getRunAndLumis(), {0: [[3, 4]]})

        testSubscription = self.createSubscription(nFiles=5, lumisPerFile=4, twoSites=False)
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=testSubscription)
        jobGroups = jobFactory(lumis_per_job=10,
                               halt_job_on_file_boundaries=False,
                               splitOnRun=False,
                               performance=self.performanceParams)
        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 2)
        self.assertEqual(jobs[0]['mask']['runAndLumis'], {0: [[0, 3]], 1: [[100, 103]], 2: [[200, 201]]})
        self.assertEqual(jobs[1]['mask']['runAndLumis'], {2: [[202, 203]], 3: [[300, 303]], 4: [[400, 403]]})

        j = Job(id=jobs[0]['id'])
        j.loadData()
        self.assertEqual(len(j['input_files']), 3)
        for f in j['input_files']:
            self.assertTrue(f['events'], 100)
            self.assertTrue(f['size'], 1000)
        return

    def createTestWorkload(self):
        """
        _createTestWorkload_

        """
        workload = newWorkload("ACDCTest")
        reco = workload.newTask("reco")
        skim1 = reco.addTask("skim1")
        workload.setOwnerDetails(name="evansde77", group="DMWM")

        # first task uses the input dataset
        reco.addInputDataset(name='/PRIMARY/processed-v1/TIERONE',
                             primary="PRIMARY", processed="processed-v1", tier="TIERONE")
        cmsRunReco = reco.makeStep("cmsRun1")
        cmsRunReco.setStepType("CMSSW")
        reco.applyTemplates()
        cmsRunRecoHelper = cmsRunReco.getTypeHelper()
        cmsRunRecoHelper.addOutputModule("outputRECO",
                                         primaryDataset="PRIMARY",
                                         processedDataset="processed-v2",
                                         dataTier="TIERTWO",
                                         lfnBase="/store/dunkindonuts",
                                         mergedLFNBase="/store/kfc")
        # second step uses an input reference
        cmsRunSkim = skim1.makeStep("cmsRun2")
        cmsRunSkim.setStepType("CMSSW")
        skim1.applyTemplates()
        skim1.setInputReference(cmsRunReco, outputModule="outputRECO")

        return workload

    def testC_ACDCTest(self):
        """
        _ACDCTest_

        Test whether we can get a goodRunList out of ACDC
        and process it correctly.
        """
        workload = self.createTestWorkload()
        dcs = DataCollectionService(url=self.testInit.couchUrl, database=self.testInit.couchDbName)

        testFileA = File(lfn=makeUUID(), size=1024, events=1024, locations="T1_US_FNAL_Disk")
        testFileA.addRun(Run(1, 1, 2))
        testFileA.create()
        testFileB = File(lfn=makeUUID(), size=1024, events=1024, locations="T1_US_FNAL_Disk")
        testFileB.addRun(Run(1, 3))
        testFileB.create()
        testJobA = getJob(workload)
        testJobA.addFile(testFileA)
        testJobA.addFile(testFileB)

        testFileC = File(lfn=makeUUID(), size=1024, events=1024, locations="T1_US_FNAL_Disk")
        testFileC.addRun(Run(1, 4, 6))
        testFileC.create()
        testJobB = getJob(workload)
        testJobB.addFile(testFileC)

        testFileD = File(lfn=makeUUID(), size=1024, events=1024, locations="T1_US_FNAL_Disk")
        testFileD.addRun(Run(1, 7))
        testFileD.create()
        testJobC = getJob(workload)
        testJobC.addFile(testFileD)

        testFileE = File(lfn=makeUUID(), size=1024, events=1024, locations="T1_US_FNAL_Disk")
        testFileE.addRun(Run(1, 11, 12))
        testFileE.create()
        testJobD = getJob(workload)
        testJobD.addFile(testFileE)

        testFileF = File(lfn=makeUUID(), size=1024, events=1024, locations="T1_US_FNAL_Disk")
        testFileF.addRun(Run(2, 5, 6, 7))
        testFileF.create()
        testJobE = getJob(workload)
        testJobE.addFile(testFileF)

        testFileG = File(lfn=makeUUID(), size=1024, events=1024, locations="T1_US_FNAL_Disk")
        testFileG.addRun(Run(2, 10, 11, 12))
        testFileG.create()
        testJobF = getJob(workload)
        testJobF.addFile(testFileG)

        testFileH = File(lfn=makeUUID(), size=1024, events=1024, locations="T1_US_FNAL_Disk")
        testFileH.addRun(Run(2, 15))
        testFileH.create()
        testJobG = getJob(workload)
        testJobG.addFile(testFileH)

        testFileI = File(lfn=makeUUID(), size=1024, events=1024, locations="T1_US_FNAL_Disk")
        testFileI.addRun(Run(3, 20))
        testFileI.create()
        testJobH = getJob(workload)
        testJobH.addFile(testFileI)

        testFileJ = File(lfn=makeUUID(), size=1024, events=1024, locations="T1_US_FNAL_Disk")
        testFileJ.addRun(Run(1, 9))
        testFileJ.create()
        testJobI = getJob(workload)
        testJobI.addFile(testFileJ)

        # dcs.failedJobs([testJobA, testJobB, testJobC, testJobD, testJobE,
        #                testJobF, testJobG, testJobH, testJobI])

        dcs.failedJobs([testJobA, testJobD, testJobH])

        baseName = makeUUID()

        testFileset = Fileset(name=baseName)
        testFileset.create()
        testFileset.addFile(testFileA)
        testFileset.addFile(testFileB)
        testFileset.addFile(testFileC)
        testFileset.addFile(testFileD)
        testFileset.addFile(testFileE)
        testFileset.addFile(testFileF)
        testFileset.addFile(testFileG)
        testFileset.addFile(testFileH)
        testFileset.addFile(testFileI)
        testFileset.addFile(testFileJ)
        testFileset.commit()

        testSubscription = Subscription(fileset=testFileset,
                                        workflow=self.testWorkflow,
                                        split_algo="LumiBased",
                                        type="Processing")
        testSubscription.create()

        splitter = SplitterFactory()
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=testSubscription)

        jobGroups = jobFactory(lumis_per_job=100,
                               halt_job_on_file_boundaries=False,
                               splitOnRun=True,
                               collectionName=workload.name(),
                               filesetName=workload.getTask("reco").getPathName(),
                               owner="evansde77",
                               group="DMWM",
                               couchURL=self.testInit.couchUrl,
                               couchDB=self.testInit.couchDbName,
                               performance=self.performanceParams)

        self.assertEqual(jobGroups[0].jobs[0]['mask'].getRunAndLumis(), {1: [[1, 2], [3, 3], [11, 12]]})
        self.assertEqual(jobGroups[0].jobs[1]['mask'].getRunAndLumis(), {3: [[20, 20]]})

        return

    def testD_NonContinuousLumis(self):
        """
        _NonContinuousLumis_

        Test and see if LumiBased can work when the lumis are non continuous
        """

        baseName = makeUUID()
        nFiles = 10

        testFileset = Fileset(name=baseName)
        testFileset.create()
        for i in range(nFiles):
            newFile = File(lfn='%s_%i' % (baseName, i), size=1000,
                           events=100, locations="T1_US_FNAL_Disk")
            # Set to two non-continuous lumi numbers
            lumis = [100 + i, 200 + i]
            newFile.addRun(Run(i, *lumis))
            newFile.create()
            testFileset.addFile(newFile)

        testFileset.commit()

        testSubscription = Subscription(fileset=testFileset,
                                        workflow=self.testWorkflow,
                                        split_algo="LumiBased",
                                        type="Processing")
        testSubscription.create()

        splitter = SplitterFactory()
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=testSubscription)

        jobGroups = jobFactory(lumis_per_job=2,
                               halt_job_on_file_boundaries=False,
                               splitOnRun=False,
                               performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 10)
        for j in jobs:
            runs = j['mask'].getRunAndLumis()
            for r in runs:
                self.assertEqual(len(runs[r]), 2)
                for l in runs[r]:
                    # Each run should have two lumis
                    # Each lumi should be of form [x, x]
                    # meaning that the first and last lumis are the same
                    self.assertEqual(len(l), 2)
                    self.assertEqual(l[0], l[1])
            self.assertEqual(j['estimatedJobTime'], 100 * 12)
            self.assertEqual(j['estimatedDiskUsage'], 100 * 400)
            self.assertEqual(j['estimatedMemoryUsage'], 2300)

        return

    def testE_getParents(self):
        """
        _getParents_

        Test the TwoFileBased version of this code
        """

        splitter = SplitterFactory()

        oneSetSubscription = self.createSubscription(nFiles=10, lumisPerFile=1)
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=oneSetSubscription)

        jobGroups = jobFactory(lumis_per_job=3,
                               split_files_between_job=True,
                               include_parents=True,
                               performance=self.performanceParams)
        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 10)
        for job in jobGroups[0].jobs:
            self.assertTrue(len(job['input_files']), 1)
            f = job['input_files'][0]
            self.assertEqual(len(f['parents']), 1)
            self.assertEqual(f['lfn'].split('_')[0],
                             list(f['parents'])[0]['lfn'].split('_')[0])

        return

    def testF_RunWhitelist(self):
        """
        _runWhitelist_

        Apparently we're too stupid to do the runlist in
        the GoodRunlist where it would make sense.
        """

        splitter = SplitterFactory()

        oneSetSubscription = self.createSubscription(nFiles=10, lumisPerFile=1)
        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=oneSetSubscription)

        jobGroups = jobFactory(lumis_per_job=10,
                               split_files_between_job=True,
                               runWhitelist=[1],
                               performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 1)
        self.assertEqual(len(jobGroups[0].jobs), 1)
        self.assertEqual(len(jobGroups[0].jobs[0]['input_files']), 1)
        self.assertEqual(len(jobGroups[0].jobs[0]['input_files'][0]['runs']), 1)
        self.assertEqual(jobGroups[0].jobs[0]['input_files'][0]['runs'][0].run, 1)
        return

    def test_NotEnoughEvents(self):
        """
        _test_NotEnoughEvents_

        Checks whether jobs are not created when there are not enough files (actually, events)
        according to the events_per_job requested to the splitter algorithm
        """
        splitter = SplitterFactory()

        # Very small fileset (single file) without enough events
        testSubscription = self.createSubscription(nFiles=1, lumisPerFile=2)

        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=testSubscription)
        jobGroups = jobFactory(lumis_per_job=5,
                               performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 0)

        # Still a small fileset (two files) without enough events
        testSubscription = self.createSubscription(nFiles=2, lumisPerFile=2)

        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=testSubscription)
        jobGroups = jobFactory(lumis_per_job=5,
                               performance=self.performanceParams,
                               splitOnRun=False)

        self.assertEqual(len(jobGroups), 0)

        # Finally an acceptable fileset size (three files) with enough events
        testSubscription = self.createSubscription(nFiles=3, lumisPerFile=2)

        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=testSubscription)
        jobGroups = jobFactory(lumis_per_job=5,
                               performance=self.performanceParams,
                               splitOnRun=False)

        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 2)
        self.assertEqual(len(jobs[0]['input_files']), 3)
        self.assertEqual(len(jobs[1]['input_files']), 1)
        self.assertEqual(jobs[0]['mask'].getRunAndLumis(), {0: [[0, 1]], 1: [[100, 101]], 2: [[200, 200]]})
        self.assertEqual(jobs[1]['mask'].getRunAndLumis(), {2: [[201, 201]]})

        # Test fileset with a single run and splitOnRun=True
        testFileset = Fileset(name="FilesetA")
        for i in range(3):
            testFile = File(lfn="/this/is/file%s" % i, size=1024, events=200, locations="T1_US_FNAL_Disk")
            lumis = [i *  2 + val for val in range(1, 3)]
            testFile.addRun(Run(1, lumis))
            testFile.create()
            testFileset.addFile(testFile)
        testFileset.create()

        testSubscription = Subscription(fileset=testFileset,
                                        workflow=self.testWorkflow,
                                        split_algo="LumiBased",
                                        type="Processing")
        testSubscription.create()

        jobFactory = splitter(package="WMCore.WMBS",
                              subscription=testSubscription)
        jobGroups = jobFactory(lumis_per_job=5,
                               performance=self.performanceParams)

        self.assertEqual(len(jobGroups), 1)
        jobs = jobGroups[0].jobs
        self.assertEqual(len(jobs), 2)
        self.assertEqual(len(jobs[0]['input_files']), 3)
        self.assertEqual(len(jobs[1]['input_files']), 1)
        self.assertEqual(jobs[0]['mask'].getRunAndLumis(), {1: [[1, 2], [3, 4], [5, 5]]})
        self.assertEqual(jobs[1]['mask'].getRunAndLumis(), {1: [[6, 6]]})

        return


if __name__ == '__main__':
    unittest.main()
