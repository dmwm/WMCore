#!/usr/bin/env python
"""
_DBSBufferFile_t_

Unit tests for the DBSBufferFile class.
"""

__revision__ = "$Id: DBSBufferFile_t.py,v 1.5 2009/10/13 20:57:14 meloam Exp $"
__version__ = "$Revision: 1.5 $"

import unittest
import os
import threading
from sets import Set

from WMCore.DAOFactory import DAOFactory

from WMQuality.TestInit import TestInit
from WMCore.DataStructs.Run import Run

from WMComponent.DBSBuffer.Database.Interface.DBSBufferFile import DBSBufferFile

class FileTest(unittest.TestCase):

    
    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Try to create all of the
        DBSBuffer tables.  Also add some dummy locations.
        """

        self.testInit = TestInit(__file__, os.getenv("DIALECT"))
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMComponent.DBSBuffer.Database"],
                                useDefault = False)

        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)

        locationAction = self.daoFactory(classname = "DBSBufferFiles.AddLocation")
        locationAction.execute(siteName = "se1.cern.ch")
        locationAction.execute(siteName = "se1.fnal.gov")        
        
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
        testFile = DBSBufferFile(lfn = "/this/is/a/lfn", size = 1024, events = 10, cksum=1111)
        testFile.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                                     appFam = "RECO", psetHash = "GIBBERISH",
                                     configContent = "MOREGIBBERISH")
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
        
        testFile = DBSBufferFile(lfn = "/this/is/a/lfn", size = 1024,
                                 events = 10, cksum=1111)
        testFile.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                              appFam = "RECO", psetHash = "GIBBERISH",
                              configContent = "MOREGIBBERISH")
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
        testFile = DBSBufferFile(lfn = "/this/is/a/lfn", size = 1024,
                                 events = 10, cksum=1111)
        testFile.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                              appFam = "RECO", psetHash = "GIBBERISH",
                              configContent = "MOREGIBBERISH")
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
        testFileParentA = DBSBufferFile(lfn = "/this/is/a/parent/lfnA", size = 1024,
                                        events = 20, cksum = 1)
        testFileParentA.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                                     appFam = "RECO", psetHash = "GIBBERISH",
                                     configContent = "MOREGIBBERISH")
        testFileParentA.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFileParentA.addRun(Run(1, *[45]))
        
        testFileParentB = DBSBufferFile(lfn = "/this/is/a/parent/lfnB", size = 1024,
                                        events = 20, cksum = 2)
        testFileParentB.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                                     appFam = "RECO", psetHash = "GIBBERISH",
                                     configContent = "MOREGIBBERISH")
        testFileParentB.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")        
        testFileParentB.addRun(Run(1, *[45]))
        
        testFileParentC = DBSBufferFile(lfn = "/this/is/a/parent/lfnC", size = 1024,
                                        events = 20, cksum = 3)
        testFileParentC.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                                     appFam = "RECO", psetHash = "GIBBERISH",
                                     configContent = "MOREGIBBERISH")
        testFileParentC.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")        
        testFileParentC.addRun(Run( 1, *[45]))
        
        testFileParentA.create()
        testFileParentB.create()
        testFileParentC.create()
        
        testFile = DBSBufferFile(lfn = "/this/is/a/lfn", size = 1024,
                                 events = 10, cksum = 1)
        testFile.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                              appFam = "RECO", psetHash = "GIBBERISH",
                              configContent = "MOREGIBBERISH")
        testFile.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFile.addRun(Run( 1, *[45]))
        testFile.create()
        
        testFile.addParent(testFileParentA["lfn"])
        testFile.addParent(testFileParentB["lfn"])
        testFile.addParent(testFileParentC["lfn"])
        
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
        testFileA = DBSBufferFile(lfn = "/this/is/a/lfn", size = 1024, events = 10,
                                  cksum = 1)
        testFileA.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                               appFam = "RECO", psetHash = "GIBBERISH",
                               configContent = "MOREGIBBERISH")
        testFileA.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFileA.create()
                                                        
        testFileB = DBSBufferFile(lfn = testFileA["lfn"])
        testFileB.load()
        testFileC = DBSBufferFile(id = testFileA["id"])
        testFileC.load()

        assert testFileA == testFileB, \
               "ERROR: File load by LFN didn't work"

        assert testFileA == testFileC, \
               "ERROR: File load by ID didn't work"

        assert type(testFileB["id"]) == int or type(testFileB["id"]) == long, \
               "ERROR: File id is not an integer type."
        assert type(testFileB["size"]) == int or type(testFileB["size"]) == long, \
               "ERROR: File size is not an integer type."
        assert type(testFileB["events"]) == int or type(testFileB["events"]) == long, \
               "ERROR: File events is not an integer type."
        assert type(testFileB["cksum"]) == int or type(testFileB["cksum"]) == long, \
               "ERROR: File cksum is not an integer type."
        
        assert type(testFileC["id"]) == int or type(testFileC["id"]) == long, \
               "ERROR: File id is not an integer type."
        assert type(testFileC["size"]) == int or type(testFileC["size"]) == long, \
               "ERROR: File size is not an integer type."
        assert type(testFileC["events"]) == int or type(testFileC["events"]) == long, \
               "ERROR: File events is not an integer type."
        assert type(testFileC["cksum"]) == int or type(testFileC["cksum"]) == long, \
               "ERROR: File cksum is not an integer type."

        testFileA.delete()
        return

    def testAddChild(self):
        """
        _testAddChild_
        
        Add a child to some parent files and make sure that all the parentage
        information is loaded/stored correctly from the database.
        """
        testFileParentA = DBSBufferFile(lfn = "/this/is/a/parent/lfnA", size = 1024,
                                        events = 20, cksum = 1)
        testFileParentA.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                                     appFam = "RECO", psetHash = "GIBBERISH",
                                     configContent = "MOREGIBBERISH")
        testFileParentA.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        
        testFileParentA.addRun(Run( 1, *[45]))
        testFileParentB = DBSBufferFile(lfn = "/this/is/a/parent/lfnB", size = 1024,
                                        events = 20, cksum = 1)
        testFileParentB.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                                     appFam = "RECO", psetHash = "GIBBERISH",
                                     configContent = "MOREGIBBERISH")
        testFileParentB.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")        
        testFileParentB.addRun(Run( 1, *[45]))
        testFileParentA.create()
        testFileParentB.create()
        
        testFileA = DBSBufferFile(lfn = "/this/is/a/lfn", size = 1024, events = 10,
                                  cksum = 1)
        testFileA.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                               appFam = "RECO", psetHash = "GIBBERISH",
                               configContent = "MOREGIBBERISH")
        testFileA.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFileA.addRun(Run( 1, *[45]))
        testFileA.create()
        
        testFileParentA.addChild("/this/is/a/lfn")
        testFileParentB.addChild("/this/is/a/lfn")
        
        testFileB = DBSBufferFile(id = testFileA["id"])
        testFileB.load(parentage = 1)
        
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
        testFileParentA = DBSBufferFile(lfn = "/this/is/a/parent/lfnA", size = 1024,
                              events = 20, cksum = 1)
        testFileParentA.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                                     appFam = "RECO", psetHash = "GIBBERISH",
                                     configContent = "MOREGIBBERISH")
        testFileParentA.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")        
        testFileParentA.addRun(Run( 1, *[45]))
        
        testFileParentB = DBSBufferFile(lfn = "/this/is/a/parent/lfnB", size = 1024,
                              events = 20, cksum = 1)
        testFileParentB.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                                     appFam = "RECO", psetHash = "GIBBERISH",
                                     configContent = "MOREGIBBERISH")
        testFileParentB.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")        
        testFileParentB.addRun(Run( 1, *[45]))
        testFileParentA.create()
        testFileParentB.create()

        testFileA = DBSBufferFile(lfn = "/this/is/a/lfn", size = 1024, events = 10,
                         cksum = 1)
        testFileA.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                               appFam = "RECO", psetHash = "GIBBERISH",
                               configContent = "MOREGIBBERISH")
        testFileA.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFileA.addRun(Run( 1, *[45]))
        testFileA.create()

        testFileParentA.addChild("/this/is/a/lfn")

        myThread = threading.currentThread()
        myThread.transaction.begin()
        
        testFileParentB.addChild("/this/is/a/lfn")

        testFileB = DBSBufferFile(id = testFileA["id"])
        testFileB.load(parentage = 1)

        goldenFiles = [testFileParentA, testFileParentB]
        for parentFile in testFileB["parents"]:
            assert parentFile in goldenFiles, \
                   "ERROR: Unknown parent file"
            goldenFiles.remove(parentFile)

        assert len(goldenFiles) == 0, \
              "ERROR: Some parents are missing"

        myThread.transaction.rollback()
        testFileB.load(parentage = 1)

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
        testFileA = DBSBufferFile(lfn = "/this/is/a/lfn", size = 1024, events = 10,
                        cksum = 1)
        testFileA.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                               appFam = "RECO", psetHash = "GIBBERISH",
                               configContent = "MOREGIBBERISH")
        testFileA.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")        
        testFileA.addRun(Run( 1, *[45]))
        testFileA.create()
        
        testFileA.setLocation(["se1.fnal.gov", "se1.cern.ch"])
        testFileA.setLocation(se = "se1.fnal.gov", immediateSave = True)
        testFileA.setLocation(["bunkse1.fnal.gov", "bunkse1.cern.ch"],
                              immediateSave = False)

        testFileB = DBSBufferFile(id = testFileA["id"])
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
        testFileA = DBSBufferFile(lfn = "/this/is/a/lfn", size = 1024, events = 10,
                                  cksum = 1)
        testFileA.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                               appFam = "RECO", psetHash = "GIBBERISH",
                               configContent = "MOREGIBBERISH")
        testFileA.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")        
        testFileA.addRun(Run( 1, *[45]))
        testFileA.create()
        
        testFileA.setLocation(["se1.fnal.gov"])

        myThread = threading.currentThread()
        myThread.transaction.begin()
        
        testFileA.setLocation(["se1.cern.ch"])
        testFileA.setLocation(["bunkse1.fnal.gov", "bunkse1.cern.ch"],
                              immediateSave = False)

        testFileB = DBSBufferFile(id = testFileA["id"])
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
        testFileA = DBSBufferFile(lfn = "/this/is/a/lfn", size = 1024, events = 10,
                                  cksum = 1, locations = Set(["se1.fnal.gov"]))
        testFileA.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                               appFam = "RECO", psetHash = "GIBBERISH",
                               configContent = "MOREGIBBERISH")
        testFileA.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")        
        testFileA.addRun(Run( 1, *[45]))
        testFileA.create()

        testFileB = DBSBufferFile(lfn = "/this/is/a/lfn2", size = 1024, events = 10,
                                  cksum = 1, locations = "se1.fnal.gov")
        testFileB.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                               appFam = "RECO", psetHash = "GIBBERISH",
                               configContent = "MOREGIBBERISH")
        testFileB.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")        
        testFileB.addRun(Run( 1, *[45]))
        testFileB.create()        

        testFileC = DBSBufferFile(id = testFileA["id"])
        testFileC.load()

        goldenLocations = ["se1.fnal.gov"]
        for location in testFileC["locations"]:
            assert location in goldenLocations, \
                   "ERROR: Unknown file location"
            goldenLocations.remove(location)
            
        assert len(goldenLocations) == 0, \
              "ERROR: Some locations are missing"

        testFileC = DBSBufferFile(id = testFileB["id"])
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
        testFile = DBSBufferFile(lfn = "/this/is/a/lfn", size = 1024, events = 10,
                                 cksum = 1, locations = "se1.fnal.gov")
        testFile.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                              appFam = "RECO", psetHash = "GIBBERISH",
                              configContent = "MOREGIBBERISH")
        testFile.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        
        testFile.create()
        runSet = Set()
        runSet.add(Run( 1, *[45]))
        runSet.add(Run( 2, *[67, 68]))
        testFile.addRunSet(runSet)
        
        assert (runSet - testFile["runs"]) == Set(), \
            "Error: addRunSet is not updating set correctly"

    def testSetBlock(self):
        """
        _testSetBlock_

        Verify that the [Set|Get]Block DAOs work correctly.
        """
        myThread = threading.currentThread()
        uploadFactory = DAOFactory(package = "WMComponent.DBSUpload.Database",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)

        createAction = uploadFactory(classname = "SetBlockStatus")
        createAction.execute(block = "someblockname", locations = ["se1.cern.ch"])

        setBlockAction = self.daoFactory(classname = "DBSBufferFiles.SetBlock")
        getBlockAction = self.daoFactory(classname = "DBSBufferFiles.GetBlock")        

        testFile = DBSBufferFile(lfn = "/this/is/a/lfn", size = 1024, events = 10,
                                 cksum = 1, locations = "se1.fnal.gov")
        testFile.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                              appFam = "RECO", psetHash = "GIBBERISH",
                              configContent = "MOREGIBBERISH")
        testFile.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        
        testFile.create()

        setBlockAction.execute(lfn = testFile["lfn"], blockName = "someblockname")
        blockName = getBlockAction.execute(lfn = testFile["lfn"])

        assert blockName[0][0] == "someblockname", \
               "Error: Incorrect block returned: %s" % blockName[0][0]
        return

    def testCountFilesDAO(self):
        """
        _testCountFilesDAO_

        Verify that the CountFiles DAO object functions correctly.
        """
        testFileA = DBSBufferFile(lfn = "/this/is/a/lfnA", size = 1024, events = 10,
                                 cksum = 1, locations = "se1.fnal.gov")
        testFileA.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                              appFam = "RECO", psetHash = "GIBBERISH",
                              configContent = "MOREGIBBERISH")
        testFileA.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFileA.create()

        testFileB = DBSBufferFile(lfn = "/this/is/a/lfnB", size = 1024, events = 10,
                                 cksum = 1, locations = "se1.fnal.gov")
        testFileB.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                              appFam = "RECO", psetHash = "GIBBERISH",
                              configContent = "MOREGIBBERISH")
        testFileB.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFileB.create()

        testFileC = DBSBufferFile(lfn = "/this/is/a/lfnC", size = 1024, events = 10,
                                 cksum = 1, locations = "se1.fnal.gov")
        testFileC.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                              appFam = "RECO", psetHash = "GIBBERISH",
                              configContent = "MOREGIBBERISH")
        testFileC.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFileC.create()                

        myThread = threading.currentThread()
        dbsBufferFactory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                                      logger = myThread.logger,
                                      dbinterface = myThread.dbi)

        countAction = dbsBufferFactory(classname = "CountFiles")

        assert countAction.execute() == 3, \
               "Error: Wrong number of files counted in DBS Buffer."

        return
        
if __name__ == "__main__":
    unittest.main() 
