#!/usr/bin/env python
"""
_ParentlessMergeBySize_t_

Unit tests for parentless WMBS merging.
"""




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

class ParentlessMergeBySizeTest(unittest.TestCase):
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

        Insert some dummy jobs, jobgroups, filesets, files and subscriptions
        into WMBS to test job creation.  Three completed job groups each
        containing several files are injected.  Another incomplete job group is
        also injected.  Also files are added to the "Mergeable" subscription as
        well as to the output fileset for their jobgroups.
        """
        locationAction = self.daoFactory(classname = "Locations.New")
        locationAction.execute(siteName = "s1", seName = "somese.cern.ch")

        changeStateDAO = self.daoFactory(classname = "Jobs.ChangeState")

        self.mergeFileset = Fileset(name = "mergeFileset")
        self.mergeFileset.create()
        self.bogusFileset = Fileset(name = "bogusFileset")
        self.bogusFileset.create()        

        mergeWorkflow = Workflow(name = "mergeWorkflow", spec = "bunk2",
                                 owner = "Steve", task="Test")
        mergeWorkflow.create()
        
        self.mergeSubscription = Subscription(fileset = self.mergeFileset,
                                              workflow = mergeWorkflow,
                                              split_algo = "ParentlessMergeBySize")
        self.mergeSubscription.create()
        self.bogusSubscription = Subscription(fileset = self.bogusFileset,
                                              workflow = mergeWorkflow,
                                              split_algo = "ParentlessMergeBySize")

        file1 = File(lfn = "file1", size = 1024, events = 1024, first_event = 0,
                     locations = set(["somese.cern.ch"]))
        file1.addRun(Run(1, *[45]))
        file1.create()
        file2 = File(lfn = "file2", size = 1024, events = 1024,
                     first_event = 1024, locations = set(["somese.cern.ch"]))
        file2.addRun(Run(1, *[45]))
        file2.create()
        file3 = File(lfn = "file3", size = 1024, events = 1024,
                     first_event = 2048, locations = set(["somese.cern.ch"]))
        file3.addRun(Run(1, *[45]))
        file3.create()
        file4 = File(lfn = "file4", size = 1024, events = 1024,
                     first_event = 3072, locations = set(["somese.cern.ch"]))
        file4.addRun(Run(1, *[45]))
        file4.create()

        fileA = File(lfn = "fileA", size = 1024, events = 1024,
                     first_event = 0, locations = set(["somese.cern.ch"]))
        fileA.addRun(Run(1, *[46]))
        fileA.create()
        fileB = File(lfn = "fileB", size = 1024, events = 1024,
                     first_event = 1024, locations = set(["somese.cern.ch"]))
        fileB.addRun(Run(1, *[46]))
        fileB.create()
        fileC = File(lfn = "fileC", size = 1024, events = 1024,
                     first_event = 2048, locations = set(["somese.cern.ch"]))
        fileC.addRun(Run(1, *[46]))
        fileC.create()
        
        fileI = File(lfn = "fileI", size = 1024, events = 1024,
                     first_event = 0, locations = set(["somese.cern.ch"]))
        fileI.addRun(Run(2, *[46]))
        fileI.create()
        fileII = File(lfn = "fileII", size = 1024, events = 1024,
                      first_event = 1024, locations = set(["somese.cern.ch"]))
        fileII.addRun(Run(2, *[46]))
        fileII.create()
        fileIII = File(lfn = "fileIII", size = 1024, events = 102400,
                       first_event = 2048, locations = set(["somese.cern.ch"]))
        fileIII.addRun(Run(2, *[46]))
        fileIII.create()
        fileIV = File(lfn = "fileIV", size = 102400, events = 1024,
                      first_event = 3072, locations = set(["somese.cern.ch"]))
        fileIV.addRun(Run(2, *[46]))
        fileIV.create()

        for file in [file1, file2, file3, file4, fileA, fileB, fileC, fileI,
                     fileII, fileIII, fileIV]:
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

        result = jobFactory(min_merge_size = 200000, max_merge_size = 2000000000,
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

        result = jobFactory(min_merge_size = 200000, max_merge_size = 2000000,
                            max_merge_events = 2000000)

        assert len(result) == 1, \
               "ERROR: More than one JobGroup returned: %s" % len(result)

        assert len(result[0].jobs) == 1, \
               "Error: One job should have been returned: %s" % len(result[0].jobs)
        
        goldenFiles = ["file1", "file2", "file3", "file4", "fileA", "fileB",
                      "fileC", "fileI", "fileII", "fileIII", "fileIV"]

        jobFiles = result[0].jobs[0].getFiles()

        currentRun = 0
        currentLumi = 0
        currentEvent = 0
        for file in jobFiles:
            file.loadData()
            assert file["lfn"] in goldenFiles, \
                   "Error: Unknown file: %s" % file["lfn"]
            assert file["locations"] == set(["somese.cern.ch"]), \
                   "Error: File is missing a location."
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

    def testMaxMergeSize(self):
        """
        _testMaxMergeSize_

        Set the maximum merge size to be 100000 bytes.  Verify that two merge
        jobs are created, one for the one large file and another for the rest of
        the files.  Verify that each merge job contains the expected files.
        """
        self.stuffWMBS()

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.mergeSubscription)

        result = jobFactory(min_merge_size = 1, max_merge_size = 100000,
                            max_merge_events = 200000)

        assert len(result) == 1, \
               "ERROR: More than one JobGroup returned: %s" % result

        assert len(result[0].jobs) == 2, \
               "ERROR: Two jobs should have been returned."

        goldenFilesA = ["file1", "file2", "file3", "file4", "fileA", "fileB",
                        "fileC", "fileI", "fileII", "fileIII"]
        goldenFilesB = ["fileIV"]

        for job in result[0].jobs:
            jobFiles = job.getFiles()
            
            if jobFiles[0]["lfn"] in goldenFilesA:
                goldenFiles = goldenFilesA
            elif jobFiles[0]["lfn"] in goldenFilesB:
                goldenFiles = goldenFilesB

            currentRun = 0
            currentLumi = 0
            currentEvent = 0
            for file in jobFiles:
                assert file["lfn"] in goldenFiles, \
                       "Error: Unknown file in merge jobs."
                assert file["locations"] == set(["somese.cern.ch"]), \
                       "Error: File is missing a location."

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

        assert len(goldenFilesA) == 0 and len(goldenFilesB) == 0, \
               "ERROR: Files missing from merge jobs."

        return

    def testMaxEvents(self):
        """
        _testMaxEvents_

        """
        self.stuffWMBS()

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.mergeSubscription)

        result = jobFactory(min_merge_size = 1, max_merge_size = 20000000,
                            max_merge_events = 100000)

        assert len(result) == 1, \
               "ERROR: More than one JobGroup returned: %s" % result

        assert len(result[0].jobs) == 2, \
               "ERROR: Two jobs should have been returned: %s" % len(result[0].jobs)

        goldenFilesA = ["file1", "file2", "file3", "file4", "fileA", "fileB",
                        "fileC", "fileI", "fileII", "fileIV"]
        goldenFilesB = ["fileIII"]

        for job in result[0].jobs:
            jobFiles = job.getFiles()
            
            if jobFiles[0]["lfn"] in goldenFilesA:
                goldenFiles = goldenFilesA
            elif jobFiles[0]["lfn"] in goldenFilesB:
                goldenFiles = goldenFilesB

            currentRun = 0
            currentLumi = 0
            currentEvent = 0
            for file in jobFiles:
                assert file["lfn"] in goldenFiles, \
                       "Error: Unknown file in merge jobs."
                assert file["locations"] == set(["somese.cern.ch"]), \
                       "Error: File is missing a location: %s" % file["locations"]

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
               "ERROR: Files missing from merge jobs."

        return

    def testLocationMerging(self):
        """
        _testLocationMerging_

        Verify that files residing on different SEs are not merged together in
        the same job.
        """
        self.stuffWMBS()

        locationAction = self.daoFactory(classname = "Locations.New")
        locationAction.execute(siteName = "s2", seName = "somese2.cern.ch")

        fileSite2 = File(lfn = "fileSite2", size = 4098, events = 1024,
                         first_event = 0, locations = set(["somese2.cern.ch"]))
        fileSite2.addRun(Run(1, *[46]))
        fileSite2.create()

        self.mergeFileset.addFile(fileSite2)
        self.mergeFileset.commit()

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = self.mergeSubscription)

        result = jobFactory(min_merge_size = 4097, max_merge_size = 99999999,
                            max_merge_events = 999999999)

        assert len(result) == 1, \
               "ERROR: More than one JobGroup returned."

        assert len(result[0].jobs) == 2, \
               "ERROR: Two jobs should have been returned."

        for job in result[0].jobs:
            firstInputFile = job.getFiles()[0]
            baseLocation = list(firstInputFile["locations"])[0]
            
            for inputFile in job.getFiles():
                assert len(inputFile["locations"]) == 1, \
                       "Error: Wrong number of locations"

                assert list(inputFile["locations"])[0] == baseLocation, \
                       "Error: Wrong location."
                       
        return
    
if __name__ == '__main__':
    unittest.main()
