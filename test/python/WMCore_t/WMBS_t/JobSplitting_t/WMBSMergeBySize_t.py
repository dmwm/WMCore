#!/usr/bin/env python
"""
_WMBSMergeBySize_t

Unit tests for generic WMBS merging.
"""

__revision__ = "$Id: WMBSMergeBySize_t.py,v 1.3 2009/03/29 23:22:38 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from sets import Set
import unittest
import os
import threading

from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow

from WMCore.DataStructs.Run import Run

from WMCore.DAOFactory import DAOFactory
from WMCore.WMFactory import WMFactory
from WMCore.JobSplitting.SplitterFactory import SplitterFactory
from WMCore.Services.UUID import makeUUID
from WMQuality.TestInit import TestInit

class EventBasedTest(unittest.TestCase):
    """
    _EventBasedTest_

    Test event based job splitting.
    """
    def runTest(self):
        """
        _runTest_

        Run all the unit tests.
        """
        unittest.main()
    
    def setUp(self):
        """
        _setUp_

        Create two subscriptions: One that contains a single file and one that
        contains multiple files.
        """
        self.testInit = TestInit(__file__, os.getenv("DIALECT"))
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)
        
        return

    def tearDown(self):
        """
        _tearDown_

        Clear out WMBS.
        """
        myThread = threading.currentThread()

        if myThread.transaction == None:
            myThread.transaction = Transaction(self.dbi)
            
        myThread.transaction.begin()
            
        factory = WMFactory("WMBS", "WMCore.WMBS")
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        
        if not destroyworked:
            raise Exception("Could not complete WMBS tear down.")
            
        myThread.transaction.commit()
        return

    def stuffWMBS(self):
        """
        _stuffWMBS_

        Insert some dummy jobs, jobgroups, filesets, files and subscriptions
        into WMBS to test merging.  Three completed job groups each containing
        several files are injected.  Another incomplete job group is also
        injected.  Also files are added to the "Mergeable" subscription as well
        as to the output fileset for their jobgroups.
        """
        bunkFileset = Fileset(name = "bunkFileset")
        bunkFileset.create()

        bunkWorkflow = Workflow(name = "bunkWorkflow", spec = "bunk",
                                owner = "Steve")
        bunkWorkflow.create()
        
        bunkSubscription = Subscription(fileset = bunkFileset,
                                        workflow = bunkWorkflow)
        bunkSubscription.create()

        jobGroup1 = JobGroup(subscription = bunkSubscription)
        jobGroup1.create()
        newJob = Job()
        newJob.create(jobGroup1)
        newJob.changeStatus("COMPLETE")
        jobGroup2 = JobGroup(subscription = bunkSubscription)
        jobGroup2.create()
        newJob = Job()
        newJob.create(jobGroup2)
        newJob.changeStatus("COMPLETE")        
        jobGroup3 = JobGroup(subscription = bunkSubscription)
        jobGroup3.create()
        newJob = Job()
        newJob.create(jobGroup3)
        newJob.changeStatus("COMPLETE")
        jobGroup4 = JobGroup(subscription = bunkSubscription)
        jobGroup4.create()
        newJob = Job()
        newJob.create(jobGroup4)        

        file1 = File(lfn = "file1", size = 1024, events = 1024, first_event = 0)
        file1.addRun(Run(1, *[45]))
        file2 = File(lfn = "file2", size = 1024, events = 1024, first_event = 1024)
        file2.addRun(Run(1, *[45]))
        file3 = File(lfn = "file3", size = 1024, events = 1024, first_event = 2048)
        file3.addRun(Run(1, *[45]))
        file4 = File(lfn = "file4", size = 1024, events = 1024, first_event = 3072)        
        file4.addRun(Run(1, *[45]))

        fileA = File(lfn = "fileA", size = 1024, events = 1024, first_event = 0)
        fileA.addRun(Run(1, *[46]))
        fileB = File(lfn = "fileB", size = 1024, events = 1024, first_event = 1024)
        fileB.addRun(Run(1, *[46]))
        fileC = File(lfn = "fileC", size = 1024, events = 1024, first_event = 2048)
        fileC.addRun(Run(1, *[46]))

        fileI = File(lfn = "fileI", size = 1024, events = 1024, first_event = 0)
        fileI.addRun(Run(2, *[46]))
        fileII = File(lfn = "fileII", size = 1024, events = 1024, first_event = 1024)
        fileII.addRun(Run(2, *[46]))
        fileIII = File(lfn = "fileIII", size = 1024, events = 1024, first_event = 2048)
        fileIII.addRun(Run(2, *[46]))
        fileIV = File(lfn = "fileIV", size = 1024, events = 1024, first_event = 3072)        
        fileIV.addRun(Run(2, *[46]))        

        fileX = File(lfn = "badFileA", size = 1024, events = 1024, first_event = 0)
        fileX.addRun(Run(1, *[47]))
        fileY = File(lfn = "badFileB", size = 1024, events = 1024, first_event = 1024)
        fileY.addRun(Run(1, *[47]))
        fileZ = File(lfn = "badFileC", size = 1024, events = 1024, first_event = 2048)
        fileZ.addRun(Run(1, *[47]))

        jobGroup1.groupoutput.addFile(file1)
        jobGroup1.groupoutput.addFile(file2)
        jobGroup1.groupoutput.addFile(file3)
        jobGroup1.groupoutput.addFile(file4)        
        jobGroup1.groupoutput.commit()

        jobGroup2.groupoutput.addFile(fileA)
        jobGroup2.groupoutput.addFile(fileB)
        jobGroup2.groupoutput.addFile(fileC)
        jobGroup2.groupoutput.commit()

        jobGroup3.groupoutput.addFile(fileI)
        jobGroup3.groupoutput.addFile(fileII)
        jobGroup3.groupoutput.addFile(fileIII)
        jobGroup3.groupoutput.addFile(fileIV)        
        jobGroup3.groupoutput.commit()                

        jobGroup4.groupoutput.addFile(fileX)
        jobGroup4.groupoutput.addFile(fileY)
        jobGroup4.groupoutput.addFile(fileZ)
        jobGroup4.groupoutput.commit()

        self.mergeFileset = Fileset(name = "mergeFileset")
        self.mergeFileset.create()

        mergeWorkflow = Workflow(name = "mergeWorkflow", spec = "bunk2",
                                 owner = "Steve")
        mergeWorkflow.create()
        
        self.mergeSubscription = Subscription(fileset = self.mergeFileset,
                                              workflow = mergeWorkflow,
                                              split_algo = "WMBSMergeBySize")
        self.mergeSubscription.create()
        
        self.mergeFileset.addFile(file1)
        self.mergeFileset.addFile(file2)
        self.mergeFileset.addFile(file3)
        self.mergeFileset.addFile(file4)
        self.mergeFileset.addFile(fileA)
        self.mergeFileset.addFile(fileB)
        self.mergeFileset.addFile(fileC)
        self.mergeFileset.addFile(fileI)
        self.mergeFileset.addFile(fileII)
        self.mergeFileset.addFile(fileIII)
        self.mergeFileset.addFile(fileIV)
        self.mergeFileset.addFile(fileX)
        self.mergeFileset.addFile(fileY)
        self.mergeFileset.addFile(fileZ)        
        self.mergeFileset.commit()

        return

    def testMinMergeSize1(self):
        """
        _testMinMergeSize1_

        Set the minimum merge size to be 20,000 bytes which is more than the
        sum of all file sizes in the WMBS instance.  Verify that no merge jobs
        will be produced.
        """
        self.stuffWMBS()

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.mergeSubscription)

        result = jobFactory(min_merge_size = 20000, max_merge_size = 200000,
                            max_merge_events = 20000)

        assert len(result) == 1, \
               "ERROR: More than one JobGroup returned."

        assert len(result[0].jobs) == 0, \
               "ERROR: No jobs should have been returned."

        return

    def testMinMergeSize1a(self):
        """
        _testMinMergeSize1a_

        Set the minimum merge size to be 20,000 bytes which is more than the
        sum of all file sizes in the WMBS instance and mark the fileset as
        closed.  Verify that one job containing all files is pushed out.
        """
        self.stuffWMBS()
        self.mergeFileset.markOpen(False)

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.mergeSubscription)

        result = jobFactory(min_merge_size = 20000, max_merge_size = 200000,
                            max_merge_events = 20000)

        assert len(result) == 1, \
               "ERROR: More than one JobGroup returned."

        assert len(result[0].jobs) == 1, \
               "ERROR: One job group should have been returned."

        jobFiles = list(result[0].jobs)[0].getFiles()

        assert len(jobFiles) == 11, \
               "ERROR: Merge job should contain 11 files."

        goldenFiles = ["file1", "file2", "file3", "file4", "fileA", "fileB",
                       "fileC", "fileI", "fileII", "fileIII", "fileIV"]

        currentRun = 0
        currentLumi = 0
        currentEvent = 0
        for file in jobFiles:
            file.loadData()
            assert file["lfn"] in goldenFiles, \
                   "ERROR: Unknown file: %s" % file["lfn"]
            goldenFiles.remove(file["lfn"])

            fileRun = list(file["runs"])[0].run
            fileLumi = min(list(file["runs"])[0])
            fileEvent = file["first_event"]

            if currentRun == 0:
                currentRun = fileRun
                currentLumi = fileLumi
                currentEvent = fileEvent
                continue

            assert fileRun >= currentRun, \
                   "ERROR: Files not sorted by run."

            if fileRun == currentRun:
                assert fileLumi >= currentLumi, \
                       "ERROR: Files not ordered by lumi"

                if fileLumi == currentLumi:
                    assert fileEvent > currentEvent, \
                           "ERROR: Files not ordered by first event"

            currentRun = fileRun
            currentLumi = fileLumi
            currentEvent = fileEvent

        return    

    def testMinMergeSize2(self):
        """
        _testMinMergeSize2_

        Set the minimum merge size to be 11,263 bytes which is one byte less
        than the sum of all the file sizes in the WMBS instance.  Verify that
        one merge job containing all the files in the WMBS instance is produced.
        """
        self.stuffWMBS()

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.mergeSubscription)

        result = jobFactory(min_merge_size = 11263, max_merge_size = 20000,
                            max_merge_events = 20000)

        assert len(result) == 1, \
               "ERROR: More than one JobGroup returned."

        assert len(result[0].jobs) == 1, \
               "ERROR: One job should have been returned."

        jobFiles = list(result[0].jobs)[0].getFiles()

        assert len(jobFiles) == 11, \
               "ERROR: Merge job should contain 11 files."

        goldenFiles = ["file1", "file2", "file3", "file4", "fileA", "fileB",
                       "fileC", "fileI", "fileII", "fileIII", "fileIV"]

        currentRun = 0
        currentLumi = 0
        currentEvent = 0
        for file in jobFiles:
            file.loadData()
            assert file["lfn"] in goldenFiles, \
                   "ERROR: Unknown file: %s" % file["lfn"]
            goldenFiles.remove(file["lfn"])

            fileRun = list(file["runs"])[0].run
            fileLumi = min(list(file["runs"])[0])
            fileEvent = file["first_event"]

            if currentRun == 0:
                currentRun = fileRun
                currentLumi = fileLumi
                currentEvent = fileEvent
                continue

            assert fileRun >= currentRun, \
                   "ERROR: Files not sorted by run."

            if fileRun == currentRun:
                assert fileLumi >= currentLumi, \
                       "ERROR: Files not ordered by lumi"

                if fileLumi == currentLumi:
                    assert fileEvent > currentEvent, \
                           "ERROR: Files not ordered by first event"

            currentRun = fileRun
            currentLumi = fileLumi
            currentEvent = fileEvent

        return

    def testMaxMergeSize1(self):
        """
        _testMaxMergeSize1_

        Set the maximum merge size to be two bytes.  Verify that three merge
        jobs are created, one for each job group that exists inside the WMBS
        instance.  Verify that each merge job contains the expected files.
        """
        self.stuffWMBS()

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.mergeSubscription)

        result = jobFactory(min_merge_size = 1, max_merge_size = 2,
                            max_merge_events = 20000)

        assert len(result) == 1, \
               "ERROR: More than one JobGroup returned."

        assert len(result[0].jobs) == 3, \
               "ERROR: Three jobs should have been returned."

        goldenFilesA = ["file1", "file2", "file3", "file4"]
        goldenFilesB = ["fileA", "fileB", "fileC"]
        goldenFilesC = ["fileI", "fileII", "fileIII", "fileIV"]

        for job in result[0].jobs:
            jobFiles = job.getFiles()
            
            if jobFiles[0]["lfn"] in goldenFilesA:
                goldenFiles = goldenFilesA
            elif jobFiles[0]["lfn"] in goldenFilesB:
                goldenFiles = goldenFilesB
            else:
                goldenFiles = goldenFilesC

            currentRun = 0
            currentLumi = 0
            currentEvent = 0
            for file in jobFiles:
                file.loadData()
                assert file["lfn"] in goldenFiles, \
                       "ERROR: Unknown file in merge jobs."

                goldenFiles.remove(file["lfn"])

            fileRun = list(file["runs"])[0].run
            fileLumi = min(list(file["runs"])[0])
            fileEvent = file["first_event"]

            if currentRun == 0:
                currentRun = fileRun
                currentLumi = fileLumi
                currentEvent = fileEvent
                continue

            assert fileRun >= currentRun, \
                   "ERROR: Files not sorted by run."

            if fileRun == currentRun:
                assert fileLumi >= currentLumi, \
                       "ERROR: Files not ordered by lumi"

                if fileLumi == currentLumi:
                    assert fileEvent > currentEvent, \
                           "ERROR: Files not ordered by first event"

            currentRun = fileRun
            currentLumi = fileLumi
            currentEvent = fileEvent

        assert len(goldenFilesA) == 0 and len(goldenFilesB) == 0 and \
               len(goldenFilesC) == 0, \
               "ERROR: Files missing from merge jobs."

        return

    def testMaxMergeSize2(self):
        """
        _testMaxMergeSize2_

        Set the minimum merge size to be one byte larger than the largest job
        group in the WMBS instance and the max merge size to be one byte larger
        than the total size of two of the groups.  Verify that one merge job
        is produced with two of the job groups in it.
        """
        self.stuffWMBS()

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.mergeSubscription)

        result = jobFactory(min_merge_size = 4097, max_merge_size = 7169,
                            max_merge_events = 20000)

        assert len(result) == 1, \
               "ERROR: More than one JobGroup returned."

        assert len(result[0].jobs) == 1, \
               "ERROR: One job should have been returned."

        goldenFilesA = ["file1", "file2", "file3", "file4"]
        goldenFilesB = ["fileA", "fileB", "fileC"]
        goldenFilesC = ["fileI", "fileII", "fileIII", "fileIV"]

        jobFiles = list(result[0].jobs)[0].getFiles()

        currentRun = 0
        currentLumi = 0
        currentEvent = 0
        for file in jobFiles:
            file.loadData()
            if file["lfn"] in goldenFilesA:
                goldenFilesA.remove(file["lfn"])
            elif file["lfn"] in goldenFilesB:
                goldenFilesB.remove(file["lfn"])
            elif file["lfn"] in goldenFilesC:
                goldenFilesC.remove(file["lfn"])

            fileRun = list(file["runs"])[0].run
            fileLumi = min(list(file["runs"])[0])
            fileEvent = file["first_event"]

            if currentRun == 0:
                currentRun = fileRun
                currentLumi = fileLumi
                currentEvent = fileEvent
                continue

            assert fileRun >= currentRun, \
                   "ERROR: Files not sorted by run."

            if fileRun == currentRun:
                assert fileLumi >= currentLumi, \
                       "ERROR: Files not ordered by lumi"

                if fileLumi == currentLumi:
                    assert fileEvent > currentEvent, \
                           "ERROR: Files not ordered by first event"

            currentRun = fileRun
            currentLumi = fileLumi
            currentEvent = fileEvent

        assert len(goldenFilesB) == 0 and \
               (len(goldenFilesA) == 0 or len(goldenFilesC) == 0), \
               "ERROR: Files not allocated to jobs correctly."

        return

    def testMaxEvents1(self):
        """
        _testMaxEvents1_

        Set the maximum number of events per merge job to 1.  Verify that three
        merge jobs are created, each one only containing the output of a single
        job group.
        """
        self.stuffWMBS()

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.mergeSubscription)

        result = jobFactory(min_merge_size = 1, max_merge_size = 20000,
                            max_merge_events = 1)

        assert len(result) == 1, \
               "ERROR: More than one JobGroup returned."
        
        assert len(result[0].jobs) == 3, \
               "ERROR: Three jobs should have been returned."

        goldenFilesA = ["file1", "file2", "file3", "file4"]
        goldenFilesB = ["fileA", "fileB", "fileC"]
        goldenFilesC = ["fileI", "fileII", "fileIII", "fileIV"]

        for job in result[0].jobs:
            jobFiles = job.getFiles()
            
            if jobFiles[0]["lfn"] in goldenFilesA:
                goldenFiles = goldenFilesA
            elif jobFiles[0]["lfn"] in goldenFilesB:
                goldenFiles = goldenFilesB
            else:
                goldenFiles = goldenFilesC

            currentRun = 0
            currentLumi = 0
            currentEvent = 0
            for file in jobFiles:
                file.loadData()
                assert file["lfn"] in goldenFiles, \
                       "ERROR: Unknown file in merge jobs."

                goldenFiles.remove(file["lfn"])

                fileRun = list(file["runs"])[0].run
                fileLumi = min(list(file["runs"])[0])
                fileEvent = file["first_event"]

                if currentRun == 0:
                    currentRun = fileRun
                    currentLumi = fileLumi
                    currentEvent = fileEvent
                    continue

                assert fileRun >= currentRun, \
                       "ERROR: Files not sorted by run."

                if fileRun == currentRun:
                    assert fileLumi >= currentLumi, \
                           "ERROR: Files not ordered by lumi"

                    if fileLumi == currentLumi:
                        assert fileEvent > currentEvent, \
                               "ERROR: Files not ordered by first event"

                currentRun = fileRun
                currentLumi = fileLumi
                currentEvent = fileEvent

        assert len(goldenFilesA) == 0 and len(goldenFilesB) == 0 and \
               len(goldenFilesC) == 0, \
               "ERROR: Files missing from merge jobs."

        return

    def testMaxEvents2(self):
        """
        _testMaxEvents2_

        Set the minimum merge size to be one byte larger than the largest job
        group in the WMBS instance and the max events to be one event larger
        than the total events in two of the groups.  Verify that one merge job
        is produced with two of the job groups in it.
        """
        self.stuffWMBS()

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.mergeSubscription)

        result = jobFactory(min_merge_size = 4097, max_merge_size = 20000,
                            max_merge_events = 7169)

        assert len(result) == 1, \
               "ERROR: More than one JobGroup returned."

        assert len(result[0].jobs) == 1, \
               "ERROR: One job should have been returned."

        goldenFilesA = ["file1", "file2", "file3", "file4"]
        goldenFilesB = ["fileA", "fileB", "fileC"]
        goldenFilesC = ["fileI", "fileII", "fileIII", "fileIV"]

        jobFiles = list(result[0].jobs)[0].getFiles()

        currentRun = 0
        currentLumi = 0
        currentEvent = 0
        for file in jobFiles:
            file.loadData()
            if file["lfn"] in goldenFilesA:
                goldenFilesA.remove(file["lfn"])
            elif file["lfn"] in goldenFilesB:
                goldenFilesB.remove(file["lfn"])
            elif file["lfn"] in goldenFilesC:
                goldenFilesC.remove(file["lfn"])

            fileRun = list(file["runs"])[0].run
            fileLumi = min(list(file["runs"])[0])
            fileEvent = file["first_event"]

            if currentRun == 0:
                currentRun = fileRun
                currentLumi = fileLumi
                currentEvent = fileEvent
                continue

            assert fileRun >= currentRun, \
                   "ERROR: Files not sorted by run."

            if fileRun == currentRun:
                assert fileLumi >= currentLumi, \
                       "ERROR: Files not ordered by lumi"

                if fileLumi == currentLumi:
                    assert fileEvent > currentEvent, \
                           "ERROR: Files not ordered by first event"

            currentRun = fileRun
            currentLumi = fileLumi
            currentEvent = fileEvent

        assert len(goldenFilesB) == 0 and \
               (len(goldenFilesA) == 0 or len(goldenFilesC) == 0), \
               "ERROR: Files not allocated to jobs correctly."

        return
    
if __name__ == '__main__':
    unittest.main()
