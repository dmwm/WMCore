#!/usr/bin/env python
"""
_DBSBufferFile_t_

Unit tests for the DBSBufferFile class.
"""

from builtins import int

import threading
import unittest

from WMComponent.DBS3Buffer.DBSBufferBlock import DBSBufferBlock
from WMComponent.DBS3Buffer.DBSBufferFile import DBSBufferFile
from WMComponent.DBS3Buffer.DBSBufferUtil import DBSBufferUtil
from WMCore.DAOFactory import DAOFactory
from WMCore.DataStructs.Run import Run
from WMQuality.TestInit import TestInit


class DBSBufferFileTest(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Try to create all of the
        DBSBuffer tables.  Also add some dummy locations.
        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules=["WMComponent.DBS3Buffer"],
                                useDefault=False)

        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package="WMComponent.DBS3Buffer",
                                     logger=myThread.logger,
                                     dbinterface=myThread.dbi)

        locationAction = self.daoFactory(classname="DBSBufferFiles.AddLocation")
        locationAction.execute(siteName="se1.cern.ch")
        locationAction.execute(siteName="se1.fnal.gov")

    def tearDown(self):
        """
        _tearDown_

        Drop all the DBSBuffer tables.
        """
        self.testInit.clearDatabase()

    def testCreateDeleteExists(self):
        """
        _testCreateDeleteExists_

        Test the create(), delete() and exists() methods of the file class
        by creating and deleting a file.  The exists() method will be
        called before and after creation and after deletion.
        """
        testFile = DBSBufferFile(lfn="/this/is/a/lfn", size=1024, events=10)
        testFile.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                              appFam="RECO", psetHash="GIBBERISH",
                              configContent="MOREGIBBERISH")
        testFile.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")

        assert testFile.exists() == False, \
            "ERROR: File exists before it was created"

        testFile.addRun(Run(1, *[45]))
        testFile.create()

        assert testFile.exists() > 0, \
            "ERROR: File does not exist after it was created"

        testFile.delete()

        assert testFile.exists() == False, \
            "ERROR: File exists after it has been deleted"
        return

    def testCreateTransaction(self):
        """
        _testCreateTransaction_

        Begin a transaction and then create a file in the database.  Afterwards,
        rollback the transaction.  Use the File class's exists() method to
        to verify that the file doesn't exist before it was created, exists
        after it was created and doesn't exist after the transaction was rolled
        back.
        """
        myThread = threading.currentThread()
        myThread.transaction.begin()

        testFile = DBSBufferFile(lfn="/this/is/a/lfn", size=1024,
                                 events=10)
        testFile.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                              appFam="RECO", psetHash="GIBBERISH",
                              configContent="MOREGIBBERISH")
        testFile.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")

        assert testFile.exists() == False, \
            "ERROR: File exists before it was created"
        testFile.addRun(Run(1, *[45]))
        testFile.create()

        assert testFile.exists() > 0, \
            "ERROR: File does not exist after it was created"

        myThread.transaction.rollback()

        assert testFile.exists() == False, \
            "ERROR: File exists after transaction was rolled back."
        return

    def testDeleteTransaction(self):
        """
        _testDeleteTransaction_

        Create a file and commit it to the database.  Start a new transaction
        and delete the file.  Rollback the transaction after the file has been
        deleted.  Use the file class's exists() method to verify that the file
        does not exist after it has been deleted but does exist after the
        transaction is rolled back.
        """
        testFile = DBSBufferFile(lfn="/this/is/a/lfn", size=1024,
                                 events=10)
        testFile.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                              appFam="RECO", psetHash="GIBBERISH",
                              configContent="MOREGIBBERISH")
        testFile.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")

        assert testFile.exists() == False, \
            "ERROR: File exists before it was created"

        testFile.addRun(Run(1, *[45]))
        testFile.create()

        assert testFile.exists() > 0, \
            "ERROR: File does not exist after it was created"

        myThread = threading.currentThread()
        myThread.transaction.begin()

        testFile.delete()

        assert testFile.exists() == False, \
            "ERROR: File exists after it has been deleted"

        myThread.transaction.rollback()

        assert testFile.exists() > 0, \
            "ERROR: File does not exist after transaction was rolled back."
        return

    def testGetParentLFNs(self):
        """
        _testGetParentLFNs_

        Create three files and set them to be parents of a fourth file.  Check
        to make sure that getParentLFNs() on the child file returns the correct
        LFNs.
        """
        testFileParentA = DBSBufferFile(lfn="/this/is/a/parent/lfnA", size=1024,
                                        events=20)
        testFileParentA.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                                     appFam="RECO", psetHash="GIBBERISH",
                                     configContent="MOREGIBBERISH")
        testFileParentA.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFileParentA.addRun(Run(1, *[45]))

        testFileParentB = DBSBufferFile(lfn="/this/is/a/parent/lfnB", size=1024,
                                        events=20)
        testFileParentB.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                                     appFam="RECO", psetHash="GIBBERISH",
                                     configContent="MOREGIBBERISH")
        testFileParentB.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFileParentB.addRun(Run(1, *[45]))

        testFileParentC = DBSBufferFile(lfn="/this/is/a/parent/lfnC", size=1024,
                                        events=20)
        testFileParentC.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                                     appFam="RECO", psetHash="GIBBERISH",
                                     configContent="MOREGIBBERISH")
        testFileParentC.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFileParentC.addRun(Run(1, *[45]))

        testFileParentA.create()
        testFileParentB.create()
        testFileParentC.create()

        testFile = DBSBufferFile(lfn="/this/is/a/lfn", size=1024,
                                 events=10)
        testFile.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                              appFam="RECO", psetHash="GIBBERISH",
                              configContent="MOREGIBBERISH")
        testFile.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFile.addRun(Run(1, *[45]))
        testFile.create()

        testFile.addParents([testFileParentA["lfn"], testFileParentB["lfn"],
                             testFileParentC["lfn"]])

        parentLFNs = testFile.getParentLFNs()

        assert len(parentLFNs) == 3, \
            "ERROR: Child does not have the right amount of parents"

        goldenLFNs = ["/this/is/a/parent/lfnA",
                      "/this/is/a/parent/lfnB",
                      "/this/is/a/parent/lfnC"]
        for parentLFN in parentLFNs:
            assert parentLFN in goldenLFNs, \
                "ERROR: Unknown parent lfn"
            goldenLFNs.remove(parentLFN)

        testFile.delete()
        testFileParentA.delete()
        testFileParentB.delete()
        testFileParentC.delete()
        return

    def testLoad(self):
        """
        _testLoad_

        Test the loading of file meta data using the ID of a file and the
        LFN of a file.
        """
        checksums = {"adler32": "adler32", "cksum": "cksum", "md5": "md5"}
        testFileA = DBSBufferFile(lfn="/this/is/a/lfn", size=1024, events=10,
                                  checksums=checksums)
        testFileA.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                               appFam="RECO", psetHash="GIBBERISH",
                               configContent="MOREGIBBERISH")
        testFileA.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFileA.create()

        testFileB = DBSBufferFile(lfn=testFileA["lfn"])
        testFileB.load()
        testFileC = DBSBufferFile(id=testFileA["id"])
        testFileC.load()

        assert testFileA == testFileB, \
            "ERROR: File load by LFN didn't work"

        assert testFileA == testFileC, \
            "ERROR: File load by ID didn't work"

        assert isinstance(testFileB["id"], int), \
            "ERROR: File id is not an integer type."
        assert isinstance(testFileB["size"], int), \
            "ERROR: File size is not an integer type."
        assert isinstance(testFileB["events"], int), \
            "ERROR: File events is not an integer type."

        assert isinstance(testFileC["id"], int), \
            "ERROR: File id is not an integer type."
        assert isinstance(testFileC["size"], int), \
            "ERROR: File size is not an integer type."
        assert isinstance(testFileC["events"], int), \
            "ERROR: File events is not an integer type."

        testFileA.delete()
        return

    def testFilesize(self):
        """
        _testFilesize_

        Test storing and loading the file information from dbsbuffer_file.
        Make sure filesize can be bigger than 32 bits
        """
        checksums = {"adler32": "adler32", "cksum": "cksum"}
        testFileA = DBSBufferFile(lfn="/this/is/a/lfn", size=3221225472, events=1500000,
                                  checksums=checksums)
        testFileA.setAlgorithm(appName="cmsRun", appVer="CMSSW_7_6_0",
                               appFam="RECO", psetHash="GIBBERISH",
                               configContent="MOREGIBBERISH")
        testFileA.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFileA.create()

        testFileB = DBSBufferFile(lfn=testFileA["lfn"])
        testFileB.load()
        self.assertEqual(testFileB["size"], 3221225472, "Error: the filesize should be 3GB")
        self.assertEqual(testFileB["events"], 1500000, "Error: the number of events should be 1.5M")

    def testAddChild(self):
        """
        _testAddChild_

        Add a child to some parent files and make sure that all the parentage
        information is loaded/stored correctly from the database.
        """
        testFileParentA = DBSBufferFile(lfn="/this/is/a/parent/lfnA", size=1024,
                                        events=20)
        testFileParentA.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                                     appFam="RECO", psetHash="GIBBERISH",
                                     configContent="MOREGIBBERISH")
        testFileParentA.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")

        testFileParentA.addRun(Run(1, *[45]))
        testFileParentB = DBSBufferFile(lfn="/this/is/a/parent/lfnB", size=1024,
                                        events=20)
        testFileParentB.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                                     appFam="RECO", psetHash="GIBBERISH",
                                     configContent="MOREGIBBERISH")
        testFileParentB.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFileParentB.addRun(Run(1, *[45]))
        testFileParentA.create()
        testFileParentB.create()

        testFileA = DBSBufferFile(lfn="/this/is/a/lfn", size=1024, events=10)
        testFileA.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                               appFam="RECO", psetHash="GIBBERISH",
                               configContent="MOREGIBBERISH")
        testFileA.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFileA.addRun(Run(1, *[45]))
        testFileA.create()

        testFileParentA.addChildren("/this/is/a/lfn")
        testFileParentB.addChildren("/this/is/a/lfn")

        testFileB = DBSBufferFile(id=testFileA["id"])
        testFileB.load(parentage=1)

        goldenFiles = [testFileParentA, testFileParentB]
        for parentFile in testFileB["parents"]:
            assert parentFile in goldenFiles, \
                "ERROR: Unknown parent file"
            goldenFiles.remove(parentFile)

        assert len(goldenFiles) == 0, \
            "ERROR: Some parents are missing"
        return

    def testAddChildTransaction(self):
        """
        _testAddChildTransaction_

        Add a child to some parent files and make sure that all the parentage
        information is loaded/stored correctly from the database.  Rollback the
        addition of one of the childs and then verify that it does in fact only
        have one parent.
        """
        testFileParentA = DBSBufferFile(lfn="/this/is/a/parent/lfnA", size=1024,
                                        events=20)
        testFileParentA.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                                     appFam="RECO", psetHash="GIBBERISH",
                                     configContent="MOREGIBBERISH")
        testFileParentA.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFileParentA.addRun(Run(1, *[45]))

        testFileParentB = DBSBufferFile(lfn="/this/is/a/parent/lfnB", size=1024,
                                        events=20)
        testFileParentB.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                                     appFam="RECO", psetHash="GIBBERISH",
                                     configContent="MOREGIBBERISH")
        testFileParentB.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFileParentB.addRun(Run(1, *[45]))
        testFileParentA.create()
        testFileParentB.create()

        testFileA = DBSBufferFile(lfn="/this/is/a/lfn", size=1024, events=10)
        testFileA.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                               appFam="RECO", psetHash="GIBBERISH",
                               configContent="MOREGIBBERISH")
        testFileA.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFileA.addRun(Run(1, *[45]))
        testFileA.create()

        testFileParentA.addChildren("/this/is/a/lfn")

        myThread = threading.currentThread()
        myThread.transaction.begin()

        testFileParentB.addChildren("/this/is/a/lfn")

        testFileB = DBSBufferFile(id=testFileA["id"])
        testFileB.load(parentage=1)

        goldenFiles = [testFileParentA, testFileParentB]
        for parentFile in testFileB["parents"]:
            assert parentFile in goldenFiles, \
                "ERROR: Unknown parent file"
            goldenFiles.remove(parentFile)

        assert len(goldenFiles) == 0, \
            "ERROR: Some parents are missing"

        myThread.transaction.rollback()
        testFileB.load(parentage=1)

        goldenFiles = [testFileParentA]
        for parentFile in testFileB["parents"]:
            assert parentFile in goldenFiles, \
                "ERROR: Unknown parent file"
            goldenFiles.remove(parentFile)

        assert len(goldenFiles) == 0, \
            "ERROR: Some parents are missing"

        return

    def testSetLocation(self):
        """
        _testSetLocation_

        Create a file and add a couple locations.  Load the file from the
        database to make sure that the locations were set correctly.
        """
        testFileA = DBSBufferFile(lfn="/this/is/a/lfn", size=1024, events=10)
        testFileA.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                               appFam="RECO", psetHash="GIBBERISH",
                               configContent="MOREGIBBERISH")
        testFileA.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFileA.addRun(Run(1, *[45]))
        testFileA.create()

        testFileA.setLocation(["se1.fnal.gov", "se1.cern.ch"])
        testFileA.setLocation(["bunkse1.fnal.gov", "bunkse1.cern.ch"],
                              immediateSave=False)

        testFileB = DBSBufferFile(id=testFileA["id"])
        testFileB.load()

        goldenLocations = ["se1.fnal.gov", "se1.cern.ch"]

        for location in testFileB["locations"]:
            assert location in goldenLocations, \
                "ERROR: Unknown file location"
            goldenLocations.remove(location)

        assert len(goldenLocations) == 0, \
            "ERROR: Some locations are missing"
        return

    def testSetLocationTransaction(self):
        """
        _testSetLocationTransaction_

        Create a file at specific locations and commit everything to the
        database.  Reload the file from the database and verify that the
        locations are correct.  Rollback the database transaction and once
        again reload the file.  Verify that the original locations are back.
        """
        testFileA = DBSBufferFile(lfn="/this/is/a/lfn", size=1024, events=10)
        testFileA.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                               appFam="RECO", psetHash="GIBBERISH",
                               configContent="MOREGIBBERISH")
        testFileA.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFileA.addRun(Run(1, *[45]))
        testFileA.create()

        testFileA.setLocation(["se1.fnal.gov"])

        myThread = threading.currentThread()
        myThread.transaction.begin()

        testFileA.setLocation(["se1.cern.ch"])
        testFileA.setLocation(["bunkse1.fnal.gov", "bunkse1.cern.ch"],
                              immediateSave=False)

        testFileB = DBSBufferFile(id=testFileA["id"])
        testFileB.load()

        goldenLocations = ["se1.fnal.gov", "se1.cern.ch"]

        for location in testFileB["locations"]:
            assert location in goldenLocations, \
                "ERROR: Unknown file location"
            goldenLocations.remove(location)

        assert len(goldenLocations) == 0, \
            "ERROR: Some locations are missing"

        myThread.transaction.rollback()
        testFileB.load()

        goldenLocations = ["se1.fnal.gov"]

        for location in testFileB["locations"]:
            assert location in goldenLocations, \
                "ERROR: Unknown file location"
            goldenLocations.remove(location)

        assert len(goldenLocations) == 0, \
            "ERROR: Some locations are missing"
        return

    def testLocationsConstructor(self):
        """
        _testLocationsConstructor_

        Test to make sure that locations passed into the File() constructor
        are loaded from and save to the database correctly.  Also test to make
        sure that the class behaves well when the location is passed in as a
        single string instead of a set.
        """
        testFileA = DBSBufferFile(lfn="/this/is/a/lfn", size=1024, events=10,
                                  locations=set(["se1.fnal.gov"]))
        testFileA.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                               appFam="RECO", psetHash="GIBBERISH",
                               configContent="MOREGIBBERISH")
        testFileA.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFileA.addRun(Run(1, *[45]))
        testFileA.create()

        testFileB = DBSBufferFile(lfn="/this/is/a/lfn2", size=1024, events=10,
                                  locations="se1.fnal.gov")
        testFileB.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                               appFam="RECO", psetHash="GIBBERISH",
                               configContent="MOREGIBBERISH")
        testFileB.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFileB.addRun(Run(1, *[45]))
        testFileB.create()

        testFileC = DBSBufferFile(id=testFileA["id"])
        testFileC.load()

        goldenLocations = ["se1.fnal.gov"]
        for location in testFileC["locations"]:
            assert location in goldenLocations, \
                "ERROR: Unknown file location"
            goldenLocations.remove(location)

        assert len(goldenLocations) == 0, \
            "ERROR: Some locations are missing"

        testFileC = DBSBufferFile(id=testFileB["id"])
        testFileC.load()

        goldenLocations = ["se1.fnal.gov"]
        for location in testFileC["locations"]:
            assert location in goldenLocations, \
                "ERROR: Unknown file location"
            goldenLocations.remove(location)

        assert len(goldenLocations) == 0, \
            "ERROR: Some locations are missing"
        return

    def testAddRunSet(self):
        """
        _testAddRunSet_

        Test the ability to add run and lumi information to a file.
        """
        testFile = DBSBufferFile(lfn="/this/is/a/lfn", size=1024, events=10,
                                 locations="se1.fnal.gov")
        testFile.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                              appFam="RECO", psetHash="GIBBERISH",
                              configContent="MOREGIBBERISH")
        testFile.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")

        testFile.create()
        runSet = set()
        runSet.add(Run(1, *[45]))
        runSet.add(Run(2, *[67, 68]))
        testFile.addRunSet(runSet)

        assert (runSet - testFile["runs"]) == set(), \
            "Error: addRunSet is not updating set correctly"

    def testSetBlock(self):
        """
        _testSetBlock_

        Verify that the [Set|Get]Block DAOs work correctly.
        """
        myThread = threading.currentThread()

        dataset = "/Cosmics/CRUZET09-PromptReco-v1/RECO"

        uploadFactory = DAOFactory(package="WMComponent.DBS3Buffer",
                                   logger=myThread.logger,
                                   dbinterface=myThread.dbi)

        datasetAction = uploadFactory(classname="NewDataset")
        createAction = uploadFactory(classname="CreateBlocks")

        datasetAction.execute(datasetPath=dataset)

        newBlock = DBSBufferBlock(name="someblockname",
                                  location="se1.cern.ch",
                                  datasetpath=None)
        newBlock.setDataset(dataset, 'data', 'VALID')

        createAction.execute(blocks=[newBlock])

        setBlockAction = self.daoFactory(classname="DBSBufferFiles.SetBlock")
        getBlockAction = self.daoFactory(classname="DBSBufferFiles.GetBlock")

        testFile = DBSBufferFile(lfn="/this/is/a/lfn", size=1024, events=10,
                                 locations="se1.fnal.gov")
        testFile.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                              appFam="RECO", psetHash="GIBBERISH",
                              configContent="MOREGIBBERISH")
        testFile.setDatasetPath(dataset)

        testFile.create()

        setBlockAction.execute(lfn=testFile["lfn"], blockName="someblockname")
        blockName = getBlockAction.execute(lfn=testFile["lfn"])

        assert blockName[0][0] == "someblockname", \
            "Error: Incorrect block returned: %s" % blockName[0][0]
        return

    def testCountFilesDAO(self):
        """
        _testCountFilesDAO_

        Verify that the CountFiles DAO object functions correctly.
        """
        testFileA = DBSBufferFile(lfn="/this/is/a/lfnA", size=1024, events=10,
                                  locations="se1.fnal.gov")
        testFileA.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                               appFam="RECO", psetHash="GIBBERISH",
                               configContent="MOREGIBBERISH")
        testFileA.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFileA.create()

        testFileB = DBSBufferFile(lfn="/this/is/a/lfnB", size=1024, events=10,
                                  locations="se1.fnal.gov")
        testFileB.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                               appFam="RECO", psetHash="GIBBERISH",
                               configContent="MOREGIBBERISH")
        testFileB.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFileB.create()

        testFileC = DBSBufferFile(lfn="/this/is/a/lfnC", size=1024, events=10,
                                  locations="se1.fnal.gov")
        testFileC.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                               appFam="RECO", psetHash="GIBBERISH",
                               configContent="MOREGIBBERISH")
        testFileC.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFileC.create()

        countAction = self.daoFactory(classname="CountFiles")

        assert countAction.execute() == 3, \
            "Error: Wrong number of files counted in DBS Buffer."

        return

    def testAddParents(self):
        """
        _testAddParents_

        Verify that the addParents() method works correctly even if the parents
        do not already exist in the database.
        """
        myThread = threading.currentThread()

        testFile = DBSBufferFile(lfn="/this/is/a/lfnA", size=1024, events=10,
                                 locations="se1.fnal.gov")
        testFile.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                              appFam="RECO", psetHash="GIBBERISH",
                              configContent="MOREGIBBERISH")
        testFile.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFile.create()

        testParent = DBSBufferFile(lfn="/this/is/a/lfnB", size=1024, events=10,
                                   locations="se1.fnal.gov")
        testParent.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                                appFam="RECO", psetHash="GIBBERISH",
                                configContent="MOREGIBBERISH")
        testParent.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RAW")
        testParent.create()

        goldenLFNs = ["lfn1", "lfn2", "lfn3", "/this/is/a/lfnB"]
        testFile.addParents(goldenLFNs)

        verifyFile = DBSBufferFile(id=testFile["id"])
        verifyFile.load(parentage=1)
        parentLFNs = verifyFile.getParentLFNs()

        for parentLFN in parentLFNs:
            self.assertTrue(parentLFN in goldenLFNs, "Error: unknown lfn %s" % parentLFN)
            goldenLFNs.remove(parentLFN)

        self.assertEqual(len(goldenLFNs), 0, "Error: missing LFNs...")

        # Check that the bogus dataset is listed as inDBS
        sqlCommand = """SELECT in_dbs FROM dbsbuffer_algo_dataset_assoc das
                          INNER JOIN dbsbuffer_dataset ds ON das.dataset_id = ds.id
                          WHERE ds.path = 'bogus'"""

        status = myThread.dbi.processData(sqlCommand)[0].fetchall()[0][0]

        self.assertEqual(status, 1)

        # Now make sure the dummy files are listed as being in DBS
        sqlCommand = """SELECT status FROM dbsbuffer_file df
                          INNER JOIN dbsbuffer_algo_dataset_assoc das ON das.id = df.dataset_algo
                          INNER JOIN dbsbuffer_dataset ds ON das.dataset_id = ds.id
                          WHERE ds.path = '/bogus/dataset/path' """

        status = myThread.dbi.processData(sqlCommand)[0].fetchall()

        for entry in status:
            self.assertEqual(entry, ('AlreadyInDBS',))

        return

    def testGetChildrenDAO(self):
        """
        _testGetChildrenDAO_

        Verify that the GetChildren DAO correctly returns the LFNs of a file's
        children.
        """
        testFileChildA = DBSBufferFile(lfn="/this/is/a/child/lfnA", size=1024,
                                       events=20)
        testFileChildA.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                                    appFam="RECO", psetHash="GIBBERISH",
                                    configContent="MOREGIBBERISH")
        testFileChildA.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFileChildB = DBSBufferFile(lfn="/this/is/a/child/lfnB", size=1024,
                                       events=20)
        testFileChildB.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                                    appFam="RECO", psetHash="GIBBERISH",
                                    configContent="MOREGIBBERISH")
        testFileChildB.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFileChildC = DBSBufferFile(lfn="/this/is/a/child/lfnC", size=1024,
                                       events=20)
        testFileChildC.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                                    appFam="RECO", psetHash="GIBBERISH",
                                    configContent="MOREGIBBERISH")
        testFileChildC.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")

        testFileChildA.create()
        testFileChildB.create()
        testFileChildC.create()

        testFile = DBSBufferFile(lfn="/this/is/a/lfn", size=1024,
                                 events=10)
        testFile.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                              appFam="RECO", psetHash="GIBBERISH",
                              configContent="MOREGIBBERISH")
        testFile.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFile.create()

        testFileChildA.addParents([testFile["lfn"]])
        testFileChildB.addParents([testFile["lfn"]])
        testFileChildC.addParents([testFile["lfn"]])

        getChildrenAction = self.daoFactory(classname="DBSBufferFiles.GetChildren")
        childLFNs = getChildrenAction.execute(testFile["lfn"])

        assert len(childLFNs) == 3, \
            "ERROR: Parent does not have the right amount of children."

        goldenLFNs = ["/this/is/a/child/lfnA",
                      "/this/is/a/child/lfnB",
                      "/this/is/a/child/lfnC"]
        for childLFN in childLFNs:
            assert childLFN in goldenLFNs, \
                "ERROR: Unknown child lfn"
            goldenLFNs.remove(childLFN)

        return

    def testGetParentStatusDAO(self):
        """
        _testGetParentStatusDAO_

        Verify that the GetParentStatus DAO correctly returns the status of a
        file's children.
        """
        testFileChild = DBSBufferFile(lfn="/this/is/a/child/lfnA", size=1024,
                                      events=20)
        testFileChild.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                                   appFam="RECO", psetHash="GIBBERISH",
                                   configContent="MOREGIBBERISH")
        testFileChild.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFileChild.create()

        testFile = DBSBufferFile(lfn="/this/is/a/lfn", size=1024,
                                 events=10)
        testFile.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                              appFam="RECO", psetHash="GIBBERISH",
                              configContent="MOREGIBBERISH")
        testFile.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFile.create()

        testFileChild.addParents([testFile["lfn"]])

        getStatusAction = self.daoFactory(classname="DBSBufferFiles.GetParentStatus")
        parentStatus = getStatusAction.execute(testFileChild["lfn"])

        assert len(parentStatus) == 1, \
            "ERROR: Wrong number of statuses returned."
        assert parentStatus[0] == "NOTUPLOADED", \
            "ERROR: Wrong status returned."

        return

    def testSetLocationByLFN(self):
        """
        _testSetLocationByLFN_

        """
        testFileA = DBSBufferFile(lfn="/this/is/a/lfn", size=1024, events=10)
        testFileA.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                               appFam="RECO", psetHash="GIBBERISH",
                               configContent="MOREGIBBERISH")
        testFileA.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFileA.addRun(Run(1, *[45]))
        testFileA.create()

        setLocationAction = self.daoFactory(classname="DBSBufferFiles.SetLocationByLFN")
        setLocationAction.execute(binds=[{'lfn': "/this/is/a/lfn", 'pnn': 'se1.cern.ch'}])

        testFileB = DBSBufferFile(id=testFileA["id"])
        testFileB.load()

        self.assertEqual(testFileB['locations'], set(['se1.cern.ch']))

        return

    def testAddCKSumByLFN(self):
        """
        _testAddCKSumByLFN_

        """

        testFileA = DBSBufferFile(lfn="/this/is/a/lfn", size=1024, events=10)
        testFileA.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                               appFam="RECO", psetHash="GIBBERISH",
                               configContent="MOREGIBBERISH")
        testFileA.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFileA.create()

        setCksumAction = self.daoFactory(classname="DBSBufferFiles.AddChecksumByLFN")
        binds = [{'lfn': "/this/is/a/lfn", 'cktype': 'adler32', 'cksum': 201},
                 {'lfn': "/this/is/a/lfn", 'cktype': 'cksum', 'cksum': 101}]
        setCksumAction.execute(bulkList=binds)

        testFileB = DBSBufferFile(id=testFileA["id"])
        testFileB.load()

        self.assertEqual(testFileB['checksums'], {'adler32': '201', 'cksum': '101'})

        return

    def testBulkLoad(self):
        """
        _testBulkLoad_

        Can we load in bulk?
        """

        addToBuffer = DBSBufferUtil()

        testFileChildA = DBSBufferFile(lfn="/this/is/a/child/lfnA", size=1024,
                                       events=20)
        testFileChildA.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                                    appFam="RECO", psetHash="GIBBERISH",
                                    configContent="MOREGIBBERISH")
        testFileChildA.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFileChildB = DBSBufferFile(lfn="/this/is/a/child/lfnB", size=1024,
                                       events=20)
        testFileChildB.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                                    appFam="RECO", psetHash="GIBBERISH",
                                    configContent="MOREGIBBERISH")
        testFileChildB.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFileChildC = DBSBufferFile(lfn="/this/is/a/child/lfnC", size=1024,
                                       events=20)
        testFileChildC.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                                    appFam="RECO", psetHash="GIBBERISH",
                                    configContent="MOREGIBBERISH")
        testFileChildC.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")

        testFileChildA.create()
        testFileChildB.create()
        testFileChildC.create()

        testFileChildA.setLocation(["se1.fnal.gov", "se1.cern.ch"])
        testFileChildB.setLocation(["se1.fnal.gov", "se1.cern.ch"])
        testFileChildC.setLocation(["se1.fnal.gov", "se1.cern.ch"])

        runSet = set()
        runSet.add(Run(1, *[45]))
        runSet.add(Run(2, *[67, 68]))
        testFileChildA.addRunSet(runSet)
        testFileChildB.addRunSet(runSet)
        testFileChildC.addRunSet(runSet)

        testFileChildA.save()
        testFileChildB.save()
        testFileChildC.save()

        setCksumAction = self.daoFactory(classname="DBSBufferFiles.AddChecksumByLFN")
        binds = [{'lfn': "/this/is/a/child/lfnA", 'cktype': 'adler32', 'cksum': 201},
                 {'lfn': "/this/is/a/child/lfnA", 'cktype': 'cksum', 'cksum': 101},
                 {'lfn': "/this/is/a/child/lfnB", 'cktype': 'adler32', 'cksum': 201},
                 {'lfn': "/this/is/a/child/lfnB", 'cktype': 'cksum', 'cksum': 101},
                 {'lfn': "/this/is/a/child/lfnC", 'cktype': 'adler32', 'cksum': 201},
                 {'lfn': "/this/is/a/child/lfnC", 'cktype': 'cksum', 'cksum': 101}]
        setCksumAction.execute(bulkList=binds)

        testFile = DBSBufferFile(lfn="/this/is/a/lfn", size=1024,
                                 events=10)
        testFile.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                              appFam="RECO", psetHash="GIBBERISH",
                              configContent="MOREGIBBERISH")
        testFile.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFile.create()

        testFileChildA.addParents([testFile["lfn"]])
        testFileChildB.addParents([testFile["lfn"]])
        testFileChildC.addParents([testFile["lfn"]])

        binds = [{'id': testFileChildA.exists()},
                 {'id': testFileChildB.exists()},
                 {'id': testFileChildC.exists()}]

        listOfFiles = addToBuffer.loadDBSBufferFilesBulk(fileObjs=binds)

        # print listOfFiles


        compareList = ['locations', 'psetHash', 'configContent', 'appName',
                       'appVer', 'appFam', 'events', 'datasetPath', 'runs']

        for f in listOfFiles:
            self.assertTrue(f['lfn'] in ["/this/is/a/child/lfnA", "/this/is/a/child/lfnB",
                                         "/this/is/a/child/lfnC"],
                            "Unknown file in loaded results")
            self.assertEqual(f['checksums'], {'adler32': '201', 'cksum': '101'})
            for parent in f['parents']:
                self.assertEqual(parent['lfn'], testFile['lfn'])
            for key in compareList:
                self.assertEqual(f[key], testFileChildA[key])

    def testProperties(self):
        """
        _testProperties_

        Test added tags that use DBSBuffer to transfer from workload to DBS
        """

        testFileA = DBSBufferFile(lfn="/this/is/a/lfn", size=1024, events=10)
        testFileA.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                               appFam="RECO", psetHash="GIBBERISH",
                               configContent="MOREGIBBERISH")
        testFileA.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFileA.setValidStatus(validStatus="VALID")
        testFileA.setProcessingVer(ver="123")
        testFileA.setAcquisitionEra(era="AcqEra")
        testFileA.setGlobalTag(globalTag="GlobalTag")
        testFileA.setDatasetParent(datasetParent="Parent")
        testFileA.create()

        return


if __name__ == "__main__":
    unittest.main()
