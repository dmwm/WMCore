#!/usr/bin/env python
"""
_File_t_

Unit tests for the WMBS File class.
"""

__revision__ = "$Id: File_t.py,v 1.39 2010/02/02 16:44:56 mnorman Exp $"
__version__ = "$Revision: 1.39 $"

import unittest
import logging
import os
import commands
import threading
import random

from WMCore.Database.DBCore    import DBInterface
from WMCore.Database.DBFactory import DBFactory
from WMCore.DAOFactory         import DAOFactory
from WMCore.WMBS.File          import File
from WMCore.WMBS.Fileset       import Fileset
from WMCore.WMBS.Workflow      import Workflow
from WMCore.WMBS.Subscription  import Subscription
from WMCore.WMBS.JobGroup      import JobGroup
from WMCore.WMFactory          import WMFactory
from WMQuality.TestInit        import TestInit
from WMCore.DataStructs.Run    import Run
from WMCore.DataStructs.File   import File as WMFile

class FileTest(unittest.TestCase):

    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Try to create all of the
        WMBS tables.  Also add some dummy locations.
        """

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)

        myThread = threading.currentThread()
        daofactory = DAOFactory(package = "WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)

        locationAction = daofactory(classname = "Locations.New")
        locationAction.execute(siteName = "se1.cern.ch")
        locationAction.execute(siteName = "se1.fnal.gov")

        return
          
    def tearDown(self):        
        """
        _tearDown_
        
        Drop all the WMBS tables.
        """
        self.testInit.clearDatabase()
        return
            
    def testCreateDeleteExists(self):
        """
        _testCreateDeleteExists_

        Test the create(), delete() and exists() methods of the file class
        by creating and deleting a file.  The exists() method will be
        called before and after creation and after deletion.
        """
        testFile = File(lfn = "/this/is/a/lfn", size = 1024, events = 10, checksums={'cksum':1111})

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
        
        testFile = File(lfn = "/this/is/a/lfn", size = 1024, events = 10, checksums={'cksum':1111})

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
        testFile = File(lfn = "/this/is/a/lfn", size = 1024, events = 10,
                        checksums={'cksum': 1111})

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

    def testGetInfo(self):
        """
        _testGetInfo_

        Test the getInfo() method of the File class to make sure that it
        returns the correct information.
        """
        testFileParent = File(lfn = "/this/is/a/parent/lfn", size = 1024,
                              events = 20, checksums={'cksum': 1111})
        testFileParent.addRun(Run(1, *[45]))
        testFileParent.create()

        testFile = File(lfn = "/this/is/a/lfn", size = 1024, events = 10, checksums={'cksum': 222})
        testFile.addRun(Run(1, *[45]))
        testFile.addRun(Run(2, *[46, 47]))
        testFile.addRun(Run(2, *[47, 48]))
        testFile.create()
        testFile.setLocation(se = "se1.fnal.gov", immediateSave = False)
        testFile.setLocation(se = "se1.cern.ch", immediateSave = False)
        testFile.addParent("/this/is/a/parent/lfn")

        info = testFile.getInfo()

        assert info[0] == testFile["lfn"], \
               "ERROR: File returned wrong LFN"
        
        assert info[1] == testFile["id"], \
               "ERROR: File returned wrong ID"
        
        assert info[2] == testFile["size"], \
               "ERROR: File returned wrong size"
        
        assert info[3] == testFile["events"], \
               "ERROR: File returned wrong events"
        
        assert info[4] == testFile["checksums"], \
               "ERROR: File returned wrong cksum"
        
        assert len(info[5]) == 2, \
		      "ERROR: File returned wrong runs"
        
        assert info[5] == [Run(1, *[45]), Run(2, *[46, 47, 48])], \
               "Error: Run hasn't been combined correctly"
               
        assert len(info[6]) == 2, \
               "ERROR: File returned wrong locations"

        for testLocation in info[6]:
            assert testLocation in ["se1.fnal.gov", "se1.cern.ch"], \
                   "ERROR: File returned wrong locations"

        assert len(info[7]) == 1, \
               "ERROR: File returned wrong parents"

        assert info[7][0] == testFileParent, \
               "ERROR: File returned wrong parents"

        testFile.delete()
        testFileParent.delete()
        return
        
    def testGetParentLFNs(self):
        """
        _testGetParentLFNs_

        Create three files and set them to be parents of a fourth file.  Check
        to make sure that getParentLFNs() on the child file returns the correct
        LFNs.
        """
        testFileParentA = File(lfn = "/this/is/a/parent/lfnA", size = 1024,
                               events = 20, checksums = {'cksum': 1})
        testFileParentA.addRun(Run(1, *[45]))
        testFileParentB = File(lfn = "/this/is/a/parent/lfnB", size = 1024,
                               events = 20, checksums = {'cksum': 2})
        testFileParentB.addRun(Run(1, *[45]))
        testFileParentC = File(lfn = "/this/is/a/parent/lfnC", size = 1024,
                               events = 20, checksums = {'cksum': 3})
        testFileParentC.addRun(Run( 1, *[45]))

        testFileParentA.create()
        testFileParentB.create()
        testFileParentC.create()

        testFile = File(lfn = "/this/is/a/lfn", size = 1024,
                        events = 10, checksums = {'cksum': 1})
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
        testFileA = File(lfn = "/this/is/a/lfn", size = 1024, events = 10,
                        checksums = {'cksum': 101}, first_event = 2, last_event = 3, merged = True)
        testFileA.create()
                                                        
        testFileB = File(lfn = testFileA["lfn"])
        testFileB.load()
        testFileC = File(id = testFileA["id"])
        testFileC.load()

        assert testFileA == testFileB, \
               "ERROR: File load by LFN didn't work"

        assert testFileA == testFileC, \
               "ERROR: File load by ID didn't work"

        assert type(testFileB["id"]) == int, \
               "ERROR: File id is not an integer type."
        assert type(testFileB["size"]) == int, \
               "ERROR: File size is not an integer type."
        assert type(testFileB["events"]) == int, \
               "ERROR: File events is not an integer type."
        assert type(testFileB["checksums"]) == dict, \
               "ERROR: File cksum is not a string type."
        assert type(testFileB["first_event"]) == int, \
               "ERROR: File first_event is not an integer type."
        assert type(testFileB["last_event"]) == int, \
               "ERROR: File last_event is not an integer type."
        
        assert type(testFileC["id"]) == int, \
               "ERROR: File id is not an integer type."
        assert type(testFileC["size"]) == int, \
               "ERROR: File size is not an integer type."
        assert type(testFileC["events"]) == int, \
               "ERROR: File events is not an integer type."
        assert type(testFileC["checksums"]) == dict, \
               "ERROR: File cksum is not an string type."
        assert type(testFileC["first_event"]) == int, \
               "ERROR: File first_event is not an integer type."
        assert type(testFileC["last_event"]) == int, \
               "ERROR: File last_event is not an integer type."

        self.assertEqual(testFileC['checksums'], {'cksum': '101'})

        testFileA.delete()
        return

    def testLoadData(self):
        """
        _testLoadData_

        Test the loading of all data from a file, including run/lumi
        associations, location information and parentage information.
        """
        testFileParentA = File(lfn = "/this/is/a/parent/lfnA", size = 1024,
                              events = 20, checksums = {'cksum': 1})
        testFileParentA.addRun(Run( 1, *[45]))
        testFileParentB = File(lfn = "/this/is/a/parent/lfnB", size = 1024,
                              events = 20, checksums = {'cksum': 1})
        testFileParentB.addRun(Run( 1, *[45]))
        testFileParentA.create()
        testFileParentB.create()

        testFileA = File(lfn = "/this/is/a/lfn", size = 1024, events = 10,
                        checksums = {'cksum':1})
        testFileA.addRun(Run( 1, *[45]))
        testFileA.create()
        testFileA.setLocation(se = "se1.fnal.gov", immediateSave = False)
        testFileA.setLocation(se = "se1.cern.ch", immediateSave = False)
        testFileA.addParent("/this/is/a/parent/lfnA")
        testFileA.addParent("/this/is/a/parent/lfnB")
        testFileA.updateLocations()
                                                        
        testFileB = File(lfn = testFileA["lfn"])
        testFileB.loadData(parentage = 1)
        testFileC = File(id = testFileA["id"])
        testFileC.loadData(parentage = 1)

        assert testFileA == testFileB, \
               "ERROR: File load by LFN didn't work"

        assert testFileA == testFileC, \
               "ERROR: File load by ID didn't work"

        testFileA.delete()
        testFileParentA.delete()
        testFileParentB.delete()
        return    

    def testAddChild(self):
        """
        _testAddChild_

        Add a child to some parent files and make sure that all the parentage
        information is loaded/stored correctly from the database.
        """
        testFileParentA = File(lfn = "/this/is/a/parent/lfnA", size = 1024,
                              events = 20, checksums = {'cksum': 1})
        testFileParentA.addRun(Run( 1, *[45]))
        testFileParentB = File(lfn = "/this/is/a/parent/lfnB", size = 1024,
                              events = 20, checksums = {'cksum': 1})
        testFileParentB.addRun(Run( 1, *[45]))
        testFileParentA.create()
        testFileParentB.create()

        testFileA = File(lfn = "/this/is/a/lfn", size = 1024, events = 10,
                         checksums = {'cksum':1})
        testFileA.addRun(Run( 1, *[45]))
        testFileA.create()

        testFileParentA.addChild("/this/is/a/lfn")
        testFileParentB.addChild("/this/is/a/lfn")

        testFileB = File(id = testFileA["id"])
        testFileB.loadData(parentage = 1)

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
        testFileParentA = File(lfn = "/this/is/a/parent/lfnA", size = 1024,
                              events = 20, checksums = {'cksum': 1})
        testFileParentA.addRun(Run( 1, *[45]))
        testFileParentB = File(lfn = "/this/is/a/parent/lfnB", size = 1024,
                              events = 20, checksums = {'cksum': 1})
        testFileParentB.addRun(Run( 1, *[45]))
        testFileParentA.create()
        testFileParentB.create()

        testFileA = File(lfn = "/this/is/a/lfn", size = 1024, events = 10,
                         checksums = {'cksum': 1})
        testFileA.addRun(Run( 1, *[45]))
        testFileA.create()

        testFileParentA.addChild("/this/is/a/lfn")

        myThread = threading.currentThread()
        myThread.transaction.begin()
        
        testFileParentB.addChild("/this/is/a/lfn")

        testFileB = File(id = testFileA["id"])
        testFileB.loadData(parentage = 1)

        goldenFiles = [testFileParentA, testFileParentB]
        for parentFile in testFileB["parents"]:
            assert parentFile in goldenFiles, \
                   "ERROR: Unknown parent file"
            goldenFiles.remove(parentFile)

        assert len(goldenFiles) == 0, \
              "ERROR: Some parents are missing"

        myThread.transaction.rollback()
        testFileB.loadData(parentage = 1)

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
        testFileA = File(lfn = "/this/is/a/lfn", size = 1024, events = 10,
                        checksums = {'cksum':1})
        testFileA.addRun(Run( 1, *[45]))
        testFileA.create()

        testFileA.setLocation(["se1.fnal.gov", "se1.cern.ch"])
        testFileA.setLocation(["bunkse1.fnal.gov", "bunkse1.cern.ch"],
                              immediateSave = False)

        testFileB = File(id = testFileA["id"])
        testFileB.loadData()

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
        testFileA = File(lfn = "/this/is/a/lfn", size = 1024, events = 10,
                        checksums = {'cksum':1})
        testFileA.addRun(Run( 1, *[45]))
        testFileA.create()
        testFileA.setLocation(["se1.fnal.gov"])

        myThread = threading.currentThread()
        myThread.transaction.begin()
        
        testFileA.setLocation(["se1.cern.ch"])
        testFileA.setLocation(["bunkse1.fnal.gov", "bunkse1.cern.ch"],
                              immediateSave = False)

        testFileB = File(id = testFileA["id"])
        testFileB.loadData()

        goldenLocations = ["se1.fnal.gov", "se1.cern.ch"]

        for location in testFileB["locations"]:
            assert location in goldenLocations, \
                   "ERROR: Unknown file location"
            goldenLocations.remove(location)

        assert len(goldenLocations) == 0, \
              "ERROR: Some locations are missing"

        myThread.transaction.rollback()
        testFileB.loadData()

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
        testFileA = File(lfn = "/this/is/a/lfn", size = 1024, events = 10,
                        checksums = {'cksum':1}, locations = set(["se1.fnal.gov"]))
        testFileA.addRun(Run( 1, *[45]))
        testFileA.create()

        testFileB = File(lfn = "/this/is/a/lfn2", size = 1024, events = 10,
                        checksums = {'cksum':1}, locations = "se1.fnal.gov")
        testFileB.addRun(Run( 1, *[45]))
        testFileB.create()        

        testFileC = File(id = testFileA["id"])
        testFileC.loadData()

        goldenLocations = ["se1.fnal.gov"]
        for location in testFileC["locations"]:
            assert location in goldenLocations, \
                   "ERROR: Unknown file location"
            goldenLocations.remove(location)
            
        assert len(goldenLocations) == 0, \
              "ERROR: Some locations are missing"

        testFileC = File(id = testFileB["id"])
        testFileC.loadData()

        goldenLocations = ["se1.fnal.gov"]
        for location in testFileC["locations"]:
            assert location in goldenLocations, \
                   "ERROR: Unknown file location"
            goldenLocations.remove(location)
            
        assert len(goldenLocations) == 0, \
              "ERROR: Some locations are missing"        
        return


    def testSetLocationOrder(self):
        """
        _testSetLocationOrder_

        This tests that you can specify a location before creating the file,
        instead of having to do it afterwards.
        """
        myThread = threading.currentThread()

        testFileA = File(lfn = "/this/is/a/lfn", size = 1024, events = 10)
        testFileA.setLocation("se1.cern.ch")
        testFileA.create()

        testFileB = File(lfn = testFileA["lfn"])
        testFileB.load()

        daoFactory = DAOFactory(package = "WMCore.WMBS", logger = logging, dbinterface = myThread.dbi)

        locationFac = daoFactory(classname = "Files.GetLocation")
        location  = locationFac.execute(testFileB['lfn']).pop()

        self.assertEqual(location, 'se1.cern.ch')

        return
        
    def testAddRunSet(self):
        """
        _testAddRunSet_

        Test the ability to add run and lumi information to a file.
        """
        testFile = File(lfn = "/this/is/a/lfn", size = 1024, events = 10,
                        checksums = {'cksum':1}, locations = "se1.fnal.gov")
        testFile.create()
        runSet = set()
        runSet.add(Run( 1, *[45]))
        runSet.add(Run( 2, *[67, 68]))
        testFile.addRunSet(runSet)
        
        assert (runSet - testFile["runs"]) == set(), \
            "Error: addRunSet is not updating set correctly"

        return
    
    def testGetAncestorLFNs(self):
        """
        _testGenAncestorLFNs_

        Create a series of files that have several generations of parentage
        information.  Verify that the parentage information is reported
        correctly.
        """
        testFileA = File(lfn = "/this/is/a/lfnA", size = 1024, events = 10,
                        checksums = {'cksum': 1}, locations = "se1.fnal.gov")
        testFileA.create()
        
        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024, events = 10,
                        checksums = {'cksum': 1}, locations = "se1.fnal.gov")
        testFileB.create()
        
        testFileC = File(lfn = "/this/is/a/lfnC", size = 1024, events = 10,
                        checksums = {'cksum': 1}, locations = "se1.fnal.gov")
        testFileC.create()
        
        testFileD = File(lfn = "/this/is/a/lfnD", size = 1024, events = 10,
                        checksums = {'cksum': 1}, locations = "se1.fnal.gov")
        testFileD.create()
        
        testFileE = File(lfn = "/this/is/a/lfnE", size = 1024, events = 10,
                        checksums = {'cksum': 1}, locations = "se1.fnal.gov")
        testFileE.create()
        
        testFileE = File(lfn = "/this/is/a/lfnF", size = 1024, events = 10,
                        checksums = {'cksum': 1}, locations = "se1.fnal.gov")
        testFileE.create()
        
        testFileA.addParent(lfn = "/this/is/a/lfnB")
        testFileA.addParent(lfn = "/this/is/a/lfnC")
        testFileB.addParent(lfn = "/this/is/a/lfnD")
        testFileC.addParent(lfn = "/this/is/a/lfnD")
        testFileD.addParent(lfn = "/this/is/a/lfnE")
        testFileD.addParent(lfn = "/this/is/a/lfnF")
        
        level1 = ["/this/is/a/lfnB", "/this/is/a/lfnC"]
        level2 = ["/this/is/a/lfnD"]
        level3 = ["/this/is/a/lfnE", "/this/is/a/lfnF"]
        level4 = level5 = []
        
        decs2 = ["/this/is/a/lfnA"]
        
        assert testFileA.getAncestors(level=1, type='lfn') == level1, \
              "ERROR: level 1 test failed"
        assert testFileA.getAncestors(level=2, type='lfn') == level2, \
              "ERROR: level 2 test failed"
        assert testFileA.getAncestors(level=3, type='lfn') == level3, \
              "ERROR: level 3 test failed"
        assert testFileA.getAncestors(level=4, type='lfn') == level4, \
              "ERROR: level 4 test failed"
        assert testFileA.getAncestors(level=5, type='lfn') == level5, \
              "ERROR: level 5 test failed"
        
        assert testFileD.getDescendants(level=1, type='lfn') == level1, \
              "ERROR: level 1 desc test failed"
        assert testFileD.getDescendants(level=2, type='lfn') == decs2, \
              "ERROR: level 2 desc test failed"
        assert testFileD.getDescendants(level=3, type='lfn') == level4, \
              "ERROR: level 3 desc test failed"

        return

    def testGetBulkLocations(self):
        """
        _testGetBulkLocations_

        Checks to see whether the code that we have will enable us to get the locations
        of all files at once
        """
        myThread = threading.currentThread()

        daoFactory = DAOFactory(package = "WMCore.WMBS", logger = logging, dbinterface = myThread.dbi)
        locationAction = daoFactory(classname = "Locations.New")
        locationAction.execute(siteName = "se2.fnal.gov")
        locationAction.execute(siteName = "se3.fnal.gov")
        locationAction.execute(siteName = "se4.fnal.gov")
        locationAction.execute(siteName = "se5.fnal.gov")
        locationAction.execute(siteName = "se6.fnal.gov")
        
        testFileA = File(lfn = "/this/is/a/lfnA", size = 1024, events = 10,
                        checksums = {'cksum': 1}, locations = "se1.fnal.gov")
        testFileA.create()
        
        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024, events = 10,
                        checksums = {'cksum': 1}, locations = "se2.fnal.gov")
        testFileB.create()
        
        testFileC = File(lfn = "/this/is/a/lfnC", size = 1024, events = 10,
                        checksums = {'cksum': 1}, locations = "se3.fnal.gov")
        testFileC.create()
        
        testFileD = File(lfn = "/this/is/a/lfnD", size = 1024, events = 10,
                        checksums = {'cksum': 1}, locations = "se4.fnal.gov")
        testFileD.create()
        
        testFileE = File(lfn = "/this/is/a/lfnE", size = 1024, events = 10,
                        checksums = {'cksum': 1}, locations = "se5.fnal.gov")
        testFileE.create()
        
        testFileF = File(lfn = "/this/is/a/lfnF", size = 1024, events = 10,
                        checksums = {'cksum': 1}, locations = "se6.fnal.gov")
        testFileF.create()

        files = [testFileA, testFileB, testFileC, testFileD, testFileE, testFileF]
        

        locationFac = daoFactory(classname = "Files.GetBulkLocation")
        location  = locationFac.execute(files = files)

        for f in files:
            self.assertEqual(location[f.exists()], list(f['locations']))

        return

    def testBulkParentage(self):
        """
        _testBulkParentage_

        Verify that the bulk parentage dao correctly sets file parentage.
        """
        testFileChildA = File(lfn = "/this/is/a/child/lfnA", size = 1024,
                              events = 20, checksums = {'cksum': 1})
        testFileChildB = File(lfn = "/this/is/a/child/lfnB", size = 1024,
                              events = 20, checksums = {'cksum': 1})
        testFileChildA.create()
        testFileChildB.create()

        testFileA = File(lfn = "/this/is/a/lfnA", size = 1024, events = 10,
                         checksums = {'cksum':1})
        testFileA.create()
        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024, events = 10,
                         checksums = {'cksum':1})
        testFileB.create()
        testFileC = File(lfn = "/this/is/a/lfnC", size = 1024, events = 10,
                         checksums = {'cksum':1})
        testFileC.create()

        parentage = [{"child": testFileChildA["id"], "parent": testFileA["id"]},
                     {"child": testFileChildA["id"], "parent": testFileB["id"]},
                     {"child": testFileChildA["id"], "parent": testFileC["id"]},
                     {"child": testFileChildB["id"], "parent": testFileA["id"]},
                     {"child": testFileChildB["id"], "parent": testFileB["id"]}]

        myThread = threading.currentThread()
        daofactory = DAOFactory(package = "WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)        
        bulkParentageAction = daofactory(classname = "Files.AddBulkParentage")
        bulkParentageAction.execute(parentage)
        
        testFileD = File(id = testFileChildA["id"])
        testFileD.loadData(parentage = 1)
        testFileE = File(id = testFileChildB["id"])
        testFileE.loadData(parentage = 1)        

        goldenFiles = [testFileA, testFileB, testFileC]
        for parentFile in testFileD["parents"]:
            assert parentFile in goldenFiles, \
                   "ERROR: Unknown parent file"
            goldenFiles.remove(parentFile)

        assert len(goldenFiles) == 0, \
              "ERROR: Some parents are missing"

        goldenFiles = [testFileA, testFileB]
        for parentFile in testFileE["parents"]:
            assert parentFile in goldenFiles, \
                   "ERROR: Unknown parent file"
            goldenFiles.remove(parentFile)

        assert len(goldenFiles) == 0, \
              "ERROR: Some parents are missing"        
        return

    def testGetMergedChildren(self):
        """
        _testGetMergedChildren_

        Verify that the GetMergedChildren DAO works correctly.  Given a parent
        file and an output fileset for the workflow the DAO will find any merged
        files that resulted from the processing workflow either as a straight to
        merge file or a file that went through a merge job.
        """
        inputFileset = Fileset(name = "inputFileset")
        outputFilesetA = Fileset(name = "outputFilesetA")
        outputFilesetB = Fileset(name = "outputFilesetB")
        mergedFilesetA = Fileset(name = "mergedFilesetA")
        mergedFilesetB = Fileset(name = "mergedFilesetB")

        inputFileset.create()
        outputFilesetA.create()
        outputFilesetB.create()
        mergedFilesetA.create()
        mergedFilesetB.create()

        procWorkflow = Workflow(spec = "wf001.xml", owner = "Steve",
                                name = "ProcWF", task = "ProcTask")
        procWorkflow.create()
        procWorkflow.addOutput("outputA", outputFilesetA)
        procWorkflow.addOutput("outputB", outputFilesetB)

        mergeAWorkflow = Workflow(spec = "wf002.xml", owner = "Steve",
                                  name = "MergeWFA", task = "MergeTask")
        mergeAWorkflow.create()
        mergeAWorkflow.addOutput("merged", mergedFilesetA)
        mergeBWorkflow = Workflow(spec = "wf003.xml", owner = "Steve",
                                  name = "MergeWFB", task = "MergeTask")
        mergeBWorkflow.create()
        mergeBWorkflow.addOutput("merged", mergedFilesetB)

        procSub = Subscription(fileset = inputFileset, workflow = procWorkflow,
                               type = "Processing")
        procSub.create()
        mergeASub = Subscription(fileset = outputFilesetA,
                                 workflow = mergeAWorkflow, type = "Merge")
        mergeASub.create()
        mergeBSub = Subscription(fileset = outputFilesetB,
                                 workflow = mergeBWorkflow, type = "Merge")
        mergeBSub.create()

        inputFileA = File(lfn = "/path/to/some/input/lfn/A", merged = True)
        inputFileA.create()
        inputFileB = File(lfn = "/path/to/some/input/lfn/B", merged = True)
        inputFileB.create()
        inputFileset.addFile(inputFileA)
        inputFileset.addFile(inputFileB)
        inputFileset.commit()

        straightMergeFile = File(lfn = "/some/straight/merged/file/A", merged = True)
        straightMergeFile.create()
        straightMergeFile.addParent(inputFileA["lfn"])
        mergedFilesetA.addFile(straightMergeFile)
        mergedFilesetA.commit()

        unmergedFileA = File(lfn = "/some/unmerged/file/A", merged = False)
        unmergedFileA.create()
        unmergedFileA.addParent(inputFileA["lfn"])
        outputFilesetA.addFile(unmergedFileA)
        outputFilesetA.commit()

        unmergedFileB = File(lfn = "/some/unmerged/file/B", merged = False)
        unmergedFileB.create()
        unmergedFileB.addParent(inputFileB["lfn"])
        outputFilesetA.addFile(unmergedFileB)
        outputFilesetA.commit()

        unmergedFileC = File(lfn = "/some/unmerged/file/C", merged = False)
        unmergedFileC.create()
        unmergedFileC.addParent(inputFileB["lfn"])
        outputFilesetB.addFile(unmergedFileC)
        outputFilesetB.commit()        
        
        mergedFileA = File(lfn = "/some/merged/file/A", merged = True)
        mergedFileA.create()
        mergedFileA.addParent(unmergedFileB["lfn"])
        mergedFilesetA.addFile(mergedFileA)
        mergedFilesetA.commit()

        mergedFileB = File(lfn = "/some/merged/file/B", merged = True)
        mergedFileB.create()
        mergedFileB.addParent(unmergedFileC["lfn"])
        mergedFilesetB.addFile(mergedFileB)
        mergedFilesetB.commit()        

        myThread = threading.currentThread()
        daofactory = DAOFactory(package = "WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)        
        mergedChildrenAction = daofactory(classname = "Files.GetMergedChildren")

        children = mergedChildrenAction.execute(inputLFN = inputFileA["lfn"],
                                                parentFileset = outputFilesetA.id)

        assert len(children) == 1, \
               "Error: Wrong number of merged children."
        assert list(children)[0] == straightMergeFile["lfn"], \
               "Error: Wrong merged child."

        children = mergedChildrenAction.execute(inputLFN = inputFileA["lfn"],
                                                parentFileset = outputFilesetB.id)

        assert len(children) == 0, \
               "Error: Wrong number of merged children."
        
        children = mergedChildrenAction.execute(inputLFN = inputFileB["lfn"],
                                                parentFileset = outputFilesetA.id)

        assert len(children) == 1, \
               "Error: Wrong number of merged children."
        assert list(children)[0] == mergedFileA["lfn"], \
               "Error: Wrong merged child."

        children = mergedChildrenAction.execute(inputLFN = inputFileB["lfn"],
                                                parentFileset = outputFilesetB.id)

        assert len(children) == 1, \
               "Error: Wrong number of merged children."
        assert list(children)[0] == mergedFileB["lfn"], \
               "Error: Wrong merged child."        

        return

    def testRedneckGetParentInfo(self):
        """
        _testRedneckGetParentInfo_

        Verify that the GetParentInfo DAO works with redneck workflows.  This
        test will create a redneck processing workflow that has two outputs.
        Each of the outputs are then set to a merge workflow.  Straight to merge
        as well as unmerged files are added to WMBS for both the child and
        parent output module.
        """
        inputFileset = Fileset(name = "inputFileset")
        outputFilesetA = Fileset(name = "outputFilesetA")
        outputFilesetB = Fileset(name = "outputFilesetB")
        mergedFilesetA = Fileset(name = "mergedFilesetA")
        mergedFilesetB = Fileset(name = "mergedFilesetB")

        inputFileset.create()
        outputFilesetA.create()
        outputFilesetB.create()
        mergedFilesetA.create()
        mergedFilesetB.create()

        procWorkflow = Workflow(spec = "wf001.xml", owner = "Steve",
                                name = "ProcWF", task = "ProcTask")
        procWorkflow.create()
        procWorkflow.addOutput("outputA", outputFilesetA)
        procWorkflow.addOutput("outputB", outputFilesetB, "outputA")

        mergeAWorkflow = Workflow(spec = "wf002.xml", owner = "Steve",
                                  name = "MergeWFA", task = "MergeTask")
        mergeAWorkflow.create()
        mergeAWorkflow.addOutput("merged", mergedFilesetA)
        mergeBWorkflow = Workflow(spec = "wf003.xml", owner = "Steve",
                                  name = "MergeWFB", task = "MergeTask")
        mergeBWorkflow.create()
        mergeBWorkflow.addOutput("merged", mergedFilesetB)

        procSub = Subscription(fileset = inputFileset, workflow = procWorkflow,
                               type = "Processing")
        procSub.create()

        mergeASub = Subscription(fileset = outputFilesetA,
                                 workflow = mergeAWorkflow, type = "Merge")
        mergeASub.create()
        mergeBSub = Subscription(fileset = outputFilesetB,
                                 workflow = mergeBWorkflow, type = "Merge")
        mergeBSub.create()

        inputFileA = File(lfn = "/path/to/some/input/lfn/A", merged = True)
        inputFileA.create()
        inputFileset.addFile(inputFileA)
        inputFileset.commit()

        inputFileB = File(lfn = "/path/to/some/input/lfn/B", merged = True)
        inputFileB.create()
        inputFileset.addFile(inputFileB)
        inputFileset.commit()        

        straightMergeParent = File(lfn = "/some/straight/merged/file/A", merged = True)
        straightMergeParent.create()
        straightMergeParent.addParent(inputFileA["lfn"])
        mergedFilesetA.addFile(straightMergeParent)
        mergedFilesetA.commit()

        straightMergeChild = File(lfn = "/some/straight/merged/file/B", merged = True)
        straightMergeChild.create()
        straightMergeChild.addParent(inputFileB["lfn"])
        mergedFilesetB.addFile(straightMergeChild)
        mergedFilesetB.commit()        

        unmergedChildFileA = File(lfn = "/some/unmerged/file/A", merged = False)
        unmergedChildFileA.create()
        unmergedChildFileA.addParent(inputFileA["lfn"])
        outputFilesetB.addFile(unmergedChildFileA)
        outputFilesetB.commit()

        unmergedChildFileB = File(lfn = "/some/unmerged/file/B", merged = False)
        unmergedChildFileB.create()
        unmergedChildFileB.addParent(inputFileB["lfn"])
        outputFilesetB.addFile(unmergedChildFileB)
        outputFilesetB.commit()        

        unmergedParentFileA = File(lfn = "/some/unmerged/parent/file/A", merged = False)
        unmergedParentFileA.create()
        unmergedParentFileA.addParent(inputFileA["lfn"])
        outputFilesetA.addFile(unmergedParentFileA)
        outputFilesetA.commit()

        unmergedParentFileB = File(lfn = "/some/unmerged/parent/file/B", merged = False)
        unmergedParentFileB.create()
        unmergedParentFileB.addParent(inputFileB["lfn"])
        outputFilesetA.addFile(unmergedParentFileB)
        outputFilesetA.commit()        

        mergedFileA = File(lfn = "/some/merged/file/A", merged = True)
        mergedFileA.create()
        mergedFileA.addParent(unmergedChildFileA["lfn"])
        mergedFileA.addParent(unmergedChildFileB["lfn"])        
        mergedFilesetB.addFile(mergedFileA)
        mergedFilesetB.commit()

        mergedFileB = File(lfn = "/some/merged/file/B", merged = True)
        mergedFileB.create()
        mergedFileB.addParent(unmergedParentFileA["lfn"])
        mergedFileB.addParent(unmergedParentFileB["lfn"])        
        mergedFilesetA.addFile(mergedFileB)
        mergedFilesetA.commit()        

        myThread = threading.currentThread()
        daofactory = DAOFactory(package = "WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)        
        parentInfoAction = daofactory(classname = "Files.GetParentInfo")

        result = parentInfoAction.execute(childLFNs = [straightMergeParent["lfn"]])

        assert len(result) == 1, \
               "Error: Wrong number of files returned."
        assert result[0]["id"] == inputFileA["id"], \
               "Error: Wrong parent ID."
        assert result[0]["lfn"] == inputFileA["lfn"], \
               "Error: Wrong parent LFN."
        assert int(result[0]["merged"]) == 1, \
               "Error: Parent file should me merged."
        assert result[0]["gplfn"] == None, \
               "Error: Grand parent should not exist."
        assert result[0]["gpmerged"] == None, \
               "Error: Grand parent should not have a merged status."
        assert result[0]["redneck_parent_fileset"] == None, \
               "Error: Redneck parent should have it's own fileset"
        assert result[0]["redneck_child_fileset"] == outputFilesetB.id, \
               "Error: Wrong redneck child fileset returned."

        result = parentInfoAction.execute(childLFNs = [straightMergeChild["lfn"]])

        assert len(result) == 1, \
               "Error: Wrong number of files returned."
        assert result[0]["id"] == inputFileB["id"], \
               "Error: Wrong parent ID."
        assert result[0]["lfn"] == inputFileB["lfn"], \
               "Error: Wrong parent LFN."
        assert int(result[0]["merged"]) == 1, \
               "Error: Parent file should me merged."
        assert result[0]["gplfn"] == None, \
               "Error: Grand parent should not exist."
        assert result[0]["gpmerged"] == None, \
               "Error: Grand parent should not have a merged status."
        assert result[0]["redneck_parent_fileset"] == outputFilesetA.id, \
               "Error: Redneck parent should have it's own fileset"
        assert result[0]["redneck_child_fileset"] == None, \
               "Error: Wrong redneck child fileset returned."

        results = parentInfoAction.execute(childLFNs = [mergedFileA["lfn"]])

        goldenParents = [unmergedChildFileA["id"], unmergedChildFileB["id"]]
        for result in results:
            assert result["id"] in goldenParents, \
                   "Error: Unknown parent."
            goldenParents.remove(result["id"])
            
            if result["id"] == unmergedChildFileA["id"]:
                assert result["lfn"] == unmergedChildFileA["lfn"], \
                       "Error: Wrong parent LFN."
                assert result["gplfn"] == inputFileA["lfn"], \
                       "Error: Grand parent lfn is incorrect."
            elif result["id"] == unmergedChildFileB["id"]:
                assert result["lfn"] == unmergedChildFileB["lfn"], \
                       "Error: Wrong parent LFN."
                assert result["gplfn"] == inputFileB["lfn"], \
                       "Error: Grand parent lfn is incorrect."

            assert int(result["merged"]) == 0, \
                   "Error: Parent file should me unmerged."
            assert int(result["gpmerged"]) == 1, \
                   "Error: Grand parent should be merged."
            assert result["redneck_parent_fileset"] == outputFilesetA.id, \
                   "Error: Redneck parent should have it's own fileset"
            assert result["redneck_child_fileset"] == None, \
                   "Error: Wrong redneck child fileset returned."                

        assert len(goldenParents) == 0, \
               "Error: Missing parents."

        results = parentInfoAction.execute(childLFNs = [mergedFileB["lfn"]])

        goldenParents = [unmergedParentFileA["id"], unmergedParentFileB["id"]]
        for result in results:
            assert result["id"] in goldenParents, \
                   "Error: Unknown parent."
            goldenParents.remove(result["id"])
            
            if result["id"] == unmergedParentFileA["id"]:
                assert result["lfn"] == unmergedParentFileA["lfn"], \
                       "Error: Wrong parent LFN."
                assert result["gplfn"] == inputFileA["lfn"], \
                       "Error: Grand parent lfn is incorrect."
            elif result["id"] == unmergedParentFileB["id"]:
                assert result["lfn"] == unmergedParentFileB["lfn"], \
                       "Error: Wrong parent LFN."
                assert result["gplfn"] == inputFileB["lfn"], \
                       "Error: Grand parent lfn is incorrect."

            assert int(result["merged"]) == 0, \
                   "Error: Parent file should me unmerged."
            assert int(result["gpmerged"]) == 1, \
                   "Error: Grand parent should be merged."
            assert result["redneck_parent_fileset"] == None, \
                   "Error: There should be no redneck parent."
            assert result["redneck_child_fileset"] == outputFilesetB.id, \
                   "Error: Wrong redneck child fileset returned."                

        assert len(goldenParents) == 0, \
               "Error: Missing parents."        

        return

    def testGetParentInfo(self):
        """
        _testGetParentInfo_

        Verify that the GetParentInfo DAO works correctly with non redneck
        workflows.  This will verify proper operation with straight to merge
        and well as unmerged files.
        """
        inputFileset = Fileset(name = "inputFileset")
        outputFileset = Fileset(name = "outputFileset")
        mergedFileset = Fileset(name = "mergedFileset")

        inputFileset.create()
        outputFileset.create()
        mergedFileset.create()

        procWorkflow = Workflow(spec = "wf001.xml", owner = "Steve",
                                name = "ProcWF", task = "ProcTask")
        procWorkflow.create()
        procWorkflow.addOutput("output", outputFileset)

        mergeWorkflow = Workflow(spec = "wf002.xml", owner = "Steve",
                                 name = "MergeWF", task = "MergeTask")
        mergeWorkflow.create()
        mergeWorkflow.addOutput("merged", mergedFileset)

        procSub = Subscription(fileset = inputFileset, workflow = procWorkflow,
                               type = "Processing")
        procSub.create()
        mergeSub = Subscription(fileset = outputFileset,
                                workflow = mergeWorkflow, type = "Merge")
        mergeSub.create()

        inputFileA = File(lfn = "/path/to/some/input/lfn/A", merged = True)
        inputFileA.create()
        inputFileB = File(lfn = "/path/to/some/input/lfn/B", merged = True)
        inputFileB.create()
        inputFileset.addFile(inputFileA)
        inputFileset.addFile(inputFileB)
        inputFileset.commit()

        straightMergeFile = File(lfn = "/some/straight/merged/file/A", merged = True)
        straightMergeFile.create()
        straightMergeFile.addParent(inputFileA["lfn"])
        mergedFileset.addFile(straightMergeFile)
        mergedFileset.commit()

        unmergedFileA = File(lfn = "/some/unmerged/file/A", merged = False)
        unmergedFileA.create()
        unmergedFileA.addParent(inputFileA["lfn"])
        outputFileset.addFile(unmergedFileA)
        outputFileset.commit()

        unmergedFileB = File(lfn = "/some/unmerged/file/B", merged = False)
        unmergedFileB.create()
        unmergedFileB.addParent(inputFileB["lfn"])
        outputFileset.addFile(unmergedFileB)
        outputFileset.commit()

        mergedFile = File(lfn = "/some/merged/file/A", merged = True)
        mergedFile.create()
        mergedFile.addParent(unmergedFileA["lfn"])        
        mergedFile.addParent(unmergedFileB["lfn"])
        mergedFileset.addFile(mergedFile)
        mergedFileset.commit()

        myThread = threading.currentThread()
        daofactory = DAOFactory(package = "WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)
        parentInfoAction = daofactory(classname = "Files.GetParentInfo")

        result = parentInfoAction.execute(childLFNs = [straightMergeFile["lfn"]])

        assert len(result) == 1, \
               "Error: Wrong number of files returned."
        assert result[0]["id"] == inputFileA["id"], \
               "Error: Wrong parent ID."
        assert result[0]["lfn"] == inputFileA["lfn"], \
               "Error: Wrong parent LFN."
        assert int(result[0]["merged"]) == 1, \
               "Error: Parent file should me merged."
        assert result[0]["gplfn"] == None, \
               "Error: Grand parent should not exist."
        assert result[0]["gpmerged"] == None, \
               "Error: Grand parent should not have a merged status."
        assert result[0]["redneck_parent_fileset"] == None, \
               "Error: There should be no redneck parent."
        assert result[0]["redneck_child_fileset"] == None, \
               "Error: There should be no redneck child."

        results = parentInfoAction.execute(childLFNs = [mergedFile["lfn"]])

        goldenParents = [unmergedFileA["id"], unmergedFileB["id"]]
        for result in results:
            assert result["id"] in goldenParents, \
                   "Error: Unknown parent."
            goldenParents.remove(result["id"])
            
            if result["id"] == unmergedFileA["id"]:
                assert result["lfn"] == unmergedFileA["lfn"], \
                       "Error: Wrong parent LFN."
                assert result["gplfn"] == inputFileA["lfn"], \
                       "Error: Grand parent lfn is incorrect."
            elif result["id"] == unmergedFileB["id"]:
                assert result["lfn"] == unmergedFileB["lfn"], \
                       "Error: Wrong parent LFN."
                assert result["gplfn"] == inputFileB["lfn"], \
                       "Error: Grand parent lfn is incorrect."

            assert int(result["merged"]) == 0, \
                   "Error: Parent file should me unmerged."
            assert int(result["gpmerged"]) == 1, \
                   "Error: Grand parent should be merged."
            assert result["redneck_parent_fileset"] == None, \
                   "Error: There should be no redneck parent."
            assert result["redneck_child_fileset"] == None, \
                   "Error: There should be no redneck children."

        assert len(goldenParents) == 0, \
               "Error: Missing parents."        
        return



    def testLoadFromDataStructsFile(self):
        """
        _testLoadFromDataStructsFile_

        Tests our ability to create a WMBS file from a DataStructs File
        """

        myThread = threading.currentThread()
        
        testLFN     = "lfn1"
        testSize    = 1024
        testEvents  = 100
        testCksum   = {"cksum": '1'}
        testParents = set(["lfn2"])
        testRun     = Run( 1, *[45])
        testSE      = "se1.cern.ch"

        parentFile = File(lfn= "lfn2")
        parentFile.create()

        testFile = File()

        inputFile = WMFile(lfn = testLFN, size = testSize, events = testEvents, checksums = testCksum, parents = testParents)
        inputFile.addRun(testRun)
        inputFile.setLocation(se = testSE)

        testFile.loadFromDataStructsFile(file = inputFile)
        testFile.create()
        testFile.save()

        
        loadFile = File(lfn = "lfn1")
        loadFile.loadData(parentage = 1)

        self.assertEqual(loadFile['size'],   testSize)
        self.assertEqual(loadFile['events'], testEvents)
        self.assertEqual(loadFile['checksums'], testCksum)
        self.assertEqual(loadFile['locations'], set([testSE]))
        self.assertEqual(loadFile['parents'].pop()['lfn'], 'lfn2')

        run = loadFile['runs'].pop()
        self.assertEqual(run, testRun)

        return
    
        
if __name__ == "__main__":
    unittest.main() 
