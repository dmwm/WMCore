#!/usr/bin/env python
"""
_WMBSMergeBySize_t

Unit tests for generic WMBS merging.
"""

__revision__ = "$Id: WMBSMergeBySize_t.py,v 1.12 2010/03/11 19:22:17 sfoulkes Exp $"
__version__ = "$Revision: 1.12 $"

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

class WMBSMergeBySize(unittest.TestCase):
    """
    _WMBSMergeBySize_

    """
    def setUp(self):
        """
        _setUp_

        Boiler plate DB setup.
        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)

        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)
        return

    def tearDown(self):
        """
        _tearDown_

        Clear out WMBS.
        """
        self.testInit.clearDatabase()
        return

    def stuffWMBS(self):
        """
        _stuffWMBS_

        """
        changeStateDAO = self.daoFactory(classname = "Jobs.ChangeState")

        inputFileset = Fileset(name = "inputFileset")
        inputFileset.create()

        inputWorkflow = Workflow(name = "inputWorkflow", spec = "input",
                                owner = "Steve", task = "Test")
        inputWorkflow.create()
        
        inputSubscription = Subscription(fileset = inputFileset,
                                        workflow = inputWorkflow)
        inputSubscription.create()

        parentFile1 = File(lfn = "parentFile1")
        parentFile1.create()
        parentFile2 = File(lfn = "parentFile2")
        parentFile2.create() 
        parentFile3 = File(lfn = "parentFile3")
        parentFile3.create()
        parentFile4 = File(lfn = "parentFile4")
        parentFile4.create()        

        jobGroup1 = JobGroup(subscription = inputSubscription)
        jobGroup1.create()
        jobGroup2 = JobGroup(subscription = inputSubscription)
        jobGroup2.create()
        
        testJob1 = Job()
        testJob1.addFile(parentFile1)
        testJob1.create(jobGroup1)
        testJob1["state"] = "cleanout"
        testJob1["oldstate"] = "new"
        testJob1["couch_record"] = "somejive"
        testJob1["retry_count"] = 0
        testJob1["outcome"] = "success"
        testJob1.save()
        changeStateDAO.execute([testJob1])
        
        testJob2 = Job()
        testJob2.addFile(parentFile2)
        testJob2.create(jobGroup1)
        testJob2["state"] = "cleanout"
        testJob2["oldstate"] = "new"
        testJob2["couch_record"] = "somejive"
        testJob2["retry_count"] = 0
        testJob2["outcome"] = "success"
        testJob2.save()        
        changeStateDAO.execute([testJob2])

        testJob3 = Job()
        testJob3.addFile(parentFile3)
        testJob3.create(jobGroup2)
        testJob3["state"] = "cleanout"
        testJob3["oldstate"] = "new"
        testJob3["couch_record"] = "somejive"
        testJob3["retry_count"] = 0
        testJob3["outcome"] = "success"
        testJob3.save()        
        changeStateDAO.execute([testJob3])

        testJob4 = Job()
        testJob4.addFile(parentFile4)
        testJob4.create(jobGroup2)
        testJob4["state"] = "cleanout"
        testJob4["oldstate"] = "new"
        testJob4["couch_record"] = "somejive"
        testJob4["retry_count"] = 0
        testJob4["outcome"] = "failure"
        testJob4.save()        
        changeStateDAO.execute([testJob4])

        # We'll simulate a failed split by event job that the merger should
        # ignore.
        parentFile5 = File(lfn = "parentFile5")
        parentFile5.create()        

        testJob5 = Job()
        testJob5.addFile(parentFile5)
        testJob5.create(jobGroup2)
        testJob5["state"] = "cleanout"
        testJob5["oldstate"] = "new"
        testJob5["couch_record"] = "somejive"
        testJob5["retry_count"] = 0
        testJob5["outcome"] = "success"
        testJob5.save()        
        changeStateDAO.execute([testJob5])

        testJob6 = Job()
        testJob6.addFile(parentFile5)
        testJob6.create(jobGroup2)
        testJob6["state"] = "cleanout"
        testJob6["oldstate"] = "new"
        testJob6["couch_record"] = "somejive"
        testJob6["retry_count"] = 0
        testJob6["outcome"] = "failure"
        testJob6.save()        
        changeStateDAO.execute([testJob6])                

        badFile1 = File(lfn = "badFile1", size = 10241024, events = 10241024, first_event = 0)
        badFile1.addRun(Run(1, *[45]))
        badFile1.create()
        badFile1.addParent(parentFile5["lfn"])

        file1 = File(lfn = "file1", size = 1024, events = 1024, first_event = 0)
        file1.addRun(Run(1, *[45]))
        file1.create()
        file1.addParent(parentFile1["lfn"])
        file2 = File(lfn = "file2", size = 1024, events = 1024, first_event = 1024)
        file2.addRun(Run(1, *[45]))
        file2.create()
        file2.addParent(parentFile1["lfn"])
        file3 = File(lfn = "file3", size = 1024, events = 1024, first_event = 2048)
        file3.addRun(Run(1, *[45]))
        file3.create()
        file3.addParent(parentFile1["lfn"])
        file4 = File(lfn = "file4", size = 1024, events = 1024, first_event = 3072)        
        file4.addRun(Run(1, *[45]))
        file4.create()
        file4.addParent(parentFile1["lfn"]) 

        fileA = File(lfn = "fileA", size = 1024, events = 1024, first_event = 0)
        fileA.addRun(Run(1, *[46]))
        fileA.create()
        fileA.addParent(parentFile2["lfn"])
        fileB = File(lfn = "fileB", size = 1024, events = 1024, first_event = 1024)
        fileB.addRun(Run(1, *[46]))
        fileB.create()
        fileB.addParent(parentFile2["lfn"])
        fileC = File(lfn = "fileC", size = 1024, events = 1024, first_event = 2048)
        fileC.addRun(Run(1, *[46]))
        fileC.create()
        fileC.addParent(parentFile2["lfn"])
        
        fileI = File(lfn = "fileI", size = 1024, events = 1024, first_event = 0)
        fileI.addRun(Run(2, *[46]))
        fileI.create()
        fileI.addParent(parentFile3["lfn"])
        fileII = File(lfn = "fileII", size = 1024, events = 1024, first_event = 1024)
        fileII.addRun(Run(2, *[46]))
        fileII.create()
        fileII.addParent(parentFile3["lfn"])
        fileIII = File(lfn = "fileIII", size = 1024, events = 1024, first_event = 2048)
        fileIII.addRun(Run(2, *[46]))
        fileIII.create()
        fileIII.addParent(parentFile3["lfn"])        
        fileIV = File(lfn = "fileIV", size = 1024, events = 1024, first_event = 3072)        
        fileIV.addRun(Run(2, *[46]))
        fileIV.create()
        fileIV.addParent(parentFile3["lfn"])

        fileX = File(lfn = "badFileA", size = 1024, events = 1024, first_event = 0)
        fileX.addRun(Run(1, *[47]))
        fileX.create()
        fileX.addParent(parentFile4["lfn"])
        fileY = File(lfn = "badFileB", size = 1024, events = 1024, first_event = 1024)
        fileY.addRun(Run(1, *[47]))
        fileY.create()
        fileY.addParent(parentFile4["lfn"])        
        fileZ = File(lfn = "badFileC", size = 1024, events = 1024, first_event = 2048)
        fileZ.addRun(Run(1, *[47]))
        fileZ.create()
        fileZ.addParent(parentFile4["lfn"])

        jobGroup1.output.addFile(file1)
        jobGroup1.output.addFile(file2)
        jobGroup1.output.addFile(file3)
        jobGroup1.output.addFile(file4)        
        jobGroup1.output.addFile(fileA)
        jobGroup1.output.addFile(fileB)
        jobGroup1.output.addFile(fileC)
        jobGroup1.output.commit()

        jobGroup2.output.addFile(fileI)
        jobGroup2.output.addFile(fileII)
        jobGroup2.output.addFile(fileIII)
        jobGroup2.output.addFile(fileIV)        
        jobGroup2.output.addFile(fileX)
        jobGroup2.output.addFile(fileY)
        jobGroup2.output.addFile(fileZ)
        jobGroup2.output.addFile(badFile1)
        jobGroup2.output.commit()

        self.mergeFileset = Fileset(name = "mergeFileset")
        self.mergeFileset.create()
        self.bogusFileset = Fileset(name = "bogusFileset")
        self.bogusFileset.create()        

        mergeWorkflow = Workflow(name = "mergeWorkflow", spec = "bunk2",
                                 owner = "Steve", task="Test")
        mergeWorkflow.create()
        
        self.mergeSubscription = Subscription(fileset = self.mergeFileset,
                                              workflow = mergeWorkflow,
                                              split_algo = "WMBSMergeBySize")
        self.mergeSubscription.create()
        self.bogusSubscription = Subscription(fileset = self.bogusFileset,
                                              workflow = mergeWorkflow,
                                              split_algo = "WMBSMergeBySize")

        for file in [file1, file2, file3, file4, fileA, fileB, fileC, fileI,
                     fileII, fileIII, fileIV, fileX, fileY, fileZ, badFile1]:
            self.mergeFileset.addFile(file)
            self.bogusFileset.addFile(file)

        self.mergeFileset.commit()
        self.bogusFileset.commit()

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

        result = jobFactory(min_merge_size = 20000, max_merge_size = 2000000000,
                            max_merge_events = 200000000)

        assert len(result) == 0, \
               "ERROR: No job groups should be returned."

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
               "ERROR: More than one JobGroup returned: %s" % len(result)

        assert len(result[0].jobs) == 2, \
               "Error: Two jobs should have been returned."
        
        goldenFilesA = ["file1", "file2", "file3", "file4", "fileA", "fileB",
                       "fileC"]
        goldenFilesB = ["fileI", "fileII", "fileIII", "fileIV"]

        for job in result[0].jobs:
            jobFiles = job.getFiles()

            if len(jobFiles) == len(goldenFilesA):
                goldenFiles = goldenFilesA
            else:
                goldenFiles = goldenFilesB

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
                    assert fileEvent >= currentEvent, \
                           "ERROR: Files not ordered by first event"

                currentRun = fileRun
                currentLumi = fileLumi
                currentEvent = fileEvent

        return    

    def testMinMergeSize2(self):
        """
        _testMinMergeSize2_

        Set the minimum merge size to be 7,167 bytes which is one byte less
        than the sum of all the file sizes in the largest merge group in the
        WMBS instance.  Verify that one merge job containing all the files in
        the largest merge group is produced.
        """
        self.stuffWMBS()

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.mergeSubscription)

        result = jobFactory(min_merge_size = 7167, max_merge_size = 20000,
                            max_merge_events = 20000)

        assert len(result) == 1, \
               "ERROR: More than one JobGroup returned: %d" % len(result)

        assert len(result[0].jobs) == 1, \
               "ERROR: One job should have been returned."

        jobFiles = list(result[0].jobs)[0].getFiles()

        goldenFiles = ["file1", "file2", "file3", "file4", "fileA", "fileB",
                       "fileC"]


        assert len(jobFiles) == len(goldenFiles), \
               "ERROR: Merge job should contain %d files." % len(goldenFiles)

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
                    assert fileEvent >= currentEvent, \
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
               "ERROR: More than one JobGroup returned: %s" % result

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
                    assert fileEvent >= currentEvent, \
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
                    assert fileEvent >= currentEvent, \
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

        Set the maximum number of events per merge job to 1.  

        """
        self.stuffWMBS()

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.mergeSubscription)

        result = jobFactory(min_merge_size = 1, max_merge_size = 20000,
                            max_merge_events = 1)

        assert len(result) == 1, \
               "ERROR: More than one JobGroup returned: %s" % result
        
        assert len(result[0].jobs) == 3, \
               "ERROR: Three jobs should have been returned: %s" % len(result[0].jobs)

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
                       "ERROR: Files not sorted by run: %s, %s" % (fileRun, currentRun)

                if fileRun == currentRun:
                    assert fileLumi >= currentLumi, \
                           "ERROR: Files not ordered by lumi"

                    if fileLumi == currentLumi:
                        assert fileEvent >= currentEvent, \
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
                    assert fileEvent >= currentEvent, \
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
