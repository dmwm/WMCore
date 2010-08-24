#!/usr/bin/env python
""" 
_Fileset_t_

Unit tests for the WMBS Fileset class.
"""

__revision__ = "$Id: Fileset_t.py,v 1.8 2009/01/16 22:26:40 sfoulkes Exp $"
__version__ = "$Revision: 1.8 $"

import unittest
import logging
import random
import os
import threading

from WMCore.WMFactory import WMFactory
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.DataStructs.Run import Run

from WMQuality.TestInit import TestInit

class Fileset_t(unittest.TestCase):
    _setup = False
    _teardown = False
    
    def setUp(self):
        """
        _setUp_
        
        Setup the database and logging connection.  Try to create all of the
        WMBS tables.
        """
        if self._setup:
            return
        
        self.testInit = TestInit(__file__, os.getenv("DIALECT"))
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)
        
        self._setup = True
        return
                                                                
    def tearDown(self):
        """
        _tearDown_
        
        Drop all the WMBS tables.
        """
        myThread = threading.currentThread()
        
        if self._teardown:
            return
        
        factory = WMFactory("WMBS", "WMCore.WMBS")
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        myThread.transaction.begin()
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete WMBS tear down.")
        myThread.transaction.commit()
        
        self._teardown = True
        return                              

    def testCreateDeleteExists(self):
        """
        _testCreateDeleteExists_

        Create a delete a fileset object while also using the exists() method
        to determine the the create() and delete() methods succeeded.
        """
        testFileset = Fileset(name = "TestFileset")

        assert testFileset.exists() == False, \
               "ERROR: Fileset exists before it was created"

        testFileset.create()

        assert testFileset.exists() >= 0, \
               "ERROR: Fileset does not exist after it was created"

        testFileset.delete()

        assert testFileset.exists() == False, \
               "ERROR: Fileset exists after it was deleted"

        return

    def testCreateTransaction(self):
        """
        _testCreateTransaction_

        Create a Fileset and commit it to the database and then roll back the
        transaction.  Use the fileset's exists() method to verify that it
        doesn't exist in the database before create() is called, that is does
        exist after create() is called and that it does not exist after the
        transaction is rolled back.
        """
        myThread = threading.currentThread()
        myThread.transaction.begin()
        
        testFileset = Fileset(name = "TestFileset")

        assert testFileset.exists() == False, \
               "ERROR: Fileset exists before it was created"

        testFileset.create()

        assert testFileset.exists() >= 0, \
               "ERROR: Fileset does not exist after it was created"

        myThread.transaction.rollback()

        assert testFileset.exists() == False, \
               "ERROR: Fileset exists after transaction was rolled back."

        return    

    def testDeleteTransaction(self):
        """
        _testDeleteTransaction_

        Create a fileset and commit it to the database.  Delete the fileset
        and verify that it is no longer in the database using the exists()
        method.  Rollback the transaction and verify with the exists() method
        that the fileset is in the database.
        """
        testFileset = Fileset(name = "TestFileset")

        assert testFileset.exists() == False, \
               "ERROR: Fileset exists before it was created"

        testFileset.create()

        assert testFileset.exists() >= 0, \
               "ERROR: Fileset does not exist after it was created"

        myThread = threading.currentThread()
        myThread.transaction.begin()

        testFileset.delete()

        assert testFileset.exists() == False, \
               "ERROR: Fileset exists after it was deleted"

        myThread.transaction.rollback()

        assert testFileset.exists() >= 0, \
               "ERROR: Fileset doesn't exist after transaction was rolled back."

        return

    def testLoad(self):
        """
        _testLoad_

        Test retrieving fileset metadata via the id and the
        name.
        """
        testFilesetA = Fileset(name = "TestFileset")
        testFilesetA.create()

        testFilesetB = Fileset(name = testFilesetA.name)
        testFilesetB.load()        
        testFilesetC = Fileset(id = testFilesetA.id)
        testFilesetC.load()

        assert type(testFilesetB.id) == int, \
               "ERROR: Fileset id is not an int."

        assert type(testFilesetC.id) == int, \
               "ERROR: Fileset id is not an int."        

        assert testFilesetB.id == testFilesetA.id, \
               "ERROR: Load from name didn't load id"

        assert testFilesetC.name == testFilesetA.name, \
               "ERROR: Load from id didn't load name"

        testFilesetA.delete()
        return

    def testLoadData(self):
        """
        _testLoadData_

        Test saving and loading all fileset information.
        """
        testFileA = File(lfn = "/this/is/a/lfnA", size = 1024,
                         events = 20, cksum = 3)
        testFileA.addRun(Run( 1, *[45]))
        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024,
                         events = 20, cksum = 3)
        testFileB.addRun(Run( 1, *[45]))
        testFileC = File(lfn = "/this/is/a/lfnC", size = 1024,
                         events = 20, cksum = 3)
        testFileC.addRun(Run( 1, *[45]))
        testFileA.create()
        testFileB.create()
        testFileC.create()

        testFilesetA = Fileset(name = "TestFileset")
        testFilesetA.create()

        testFilesetA.addFile(testFileA)
        testFilesetA.addFile(testFileB)
        testFilesetA.addFile(testFileC)
        testFilesetA.commit()

        testFilesetB = Fileset(name = testFilesetA.name)
        testFilesetB.loadData()        
        testFilesetC = Fileset(id = testFilesetA.id)
        testFilesetC.loadData()

        assert testFilesetB.id == testFilesetA.id, \
               "ERROR: Load from name didn't load id"

        assert testFilesetC.name == testFilesetA.name, \
               "ERROR: Load from id didn't load name"

        goldenFiles = [testFileA, testFileB, testFileC]
        for filesetFile in testFilesetB.files:
            assert filesetFile in goldenFiles, \
                   "ERROR: Unknown file in fileset"
            goldenFiles.remove(filesetFile)

        assert len(goldenFiles) == 0, \
               "ERROR: Fileset is missing files"

        goldenFiles = [testFileA, testFileB, testFileC]
        for filesetFile in testFilesetC.files:
            assert filesetFile in goldenFiles, \
                   "ERROR: Unknown file in fileset"
            goldenFiles.remove(filesetFile)

        assert len(goldenFiles) == 0, \
               "ERROR: Fileset is missing files"
        
        testFilesetA.delete()
        testFileA.delete()
        testFileB.delete()
        testFileC.delete()        
        
    def testGetFiles(self):
        """
        _testGetFiles_

        Create a fileset with three files and exercise the getFiles() method to
        make sure that all the results it returns are consistant.
        """
        testFileA = File(lfn = "/this/is/a/lfnA", size = 1024,
                         events = 20, cksum = 3)
        testFileA.addRun(Run( 1, *[45]))
        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024,
                         events = 20, cksum = 3)
        testFileB.addRun(Run( 1, *[45]))
        testFileC = File(lfn = "/this/is/a/lfnC", size = 1024,
                         events = 20, cksum = 3)
        testFileC.addRun(Run( 1, *[45]))
        testFileA.create()
        testFileB.create()
        testFileC.create()

        testFilesetA = Fileset(name = "TestFileset")
        testFilesetA.create()
        
        testFilesetA.addFile(testFileA)
        testFilesetA.addFile(testFileB)
        testFilesetA.addFile(testFileC)
        testFilesetA.commit()

        filesetFiles = testFilesetA.getFiles(type = "list")

        goldenFiles = [testFileA, testFileB, testFileC]
        for filesetFile in filesetFiles:
            assert filesetFile in goldenFiles, \
                   "ERROR: Unknown file in fileset"
            goldenFiles.remove(filesetFile)

        assert len(goldenFiles) == 0, \
               "ERROR: Not all files in fileset"

        filesetFiles = testFilesetA.getFiles(type = "set")

        goldenFiles = [testFileA, testFileB, testFileC]
        for filesetFile in filesetFiles:
            assert filesetFile in goldenFiles, \
                   "ERROR: Unknown file in fileset"
            goldenFiles.remove(filesetFile)

        assert len(goldenFiles) == 0, \
               "ERROR: Not all files in fileset"

        filesetLFNs = testFilesetA.getFiles(type = "lfn")

        goldenLFNs = [testFileA["lfn"], testFileB["lfn"], testFileC["lfn"]]
        for filesetLFN in filesetLFNs:
            assert filesetLFN in goldenLFNs, \
                   "ERROR: Unknown lfn in fileset"
            goldenLFNs.remove(filesetLFN)

        assert len(goldenLFNs) == 0, \
               "ERROR: Not all lfns in fileset"

        filesetIDs = testFilesetA.getFiles(type = "id")

        goldenIDs = [testFileA["id"], testFileB["id"], testFileC["id"]]
        for filesetID in filesetIDs:
            assert filesetID in goldenIDs, \
                   "ERROR: Unknown id in fileset"
            goldenIDs.remove(filesetID)

        assert len(goldenIDs) == 0, \
               "ERROR: Not all ids in fileset"        

    def testFileCreate(self):
        """
        _testFileCreate_

        Create several files and add them to the fileset.  Test to make sure
        that the commit() fileset method will add the files to the database
        if they are not in the database.
        """
        testFileA = File(lfn = "/this/is/a/lfnA", size = 1024,
                         events = 20, cksum = 3)
        testFileA.addRun(Run( 1, *[45]))
        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024,
                         events = 20, cksum = 3)
        testFileB.addRun(Run( 1, *[45]))
        testFileC = File(lfn = "/this/is/a/lfnC", size = 1024,
                         events = 20, cksum = 3)
        testFileC.addRun(Run( 1, *[45]))
        testFileC.create()

        testFilesetA = Fileset(name = "TestFileset")
        testFilesetA.create()

        testFilesetA.addFile(testFileA)
        testFilesetA.addFile(testFileB)
        testFilesetA.addFile(testFileC)
        testFilesetA.commit()

        testFilesetB = Fileset(name = testFilesetA.name)
        testFilesetB.loadData()        
        testFilesetC = Fileset(id = testFilesetA.id)
        testFilesetC.loadData()

        assert testFilesetB.id == testFilesetA.id, \
               "ERROR: Load from name didn't load id"

        assert testFilesetC.name == testFilesetA.name, \
               "ERROR: Load from id didn't load name"

        goldenFiles = [testFileA, testFileB, testFileC]
        for filesetFile in testFilesetB.files:
            assert filesetFile in goldenFiles, \
                   "ERROR: Unknown file in fileset"
            goldenFiles.remove(filesetFile)

        assert len(goldenFiles) == 0, \
               "ERROR: Fileset is missing files"

        goldenFiles = [testFileA, testFileB, testFileC]
        for filesetFile in testFilesetC.files:
            assert filesetFile in goldenFiles, \
                   "ERROR: Unknown file in fileset"
            goldenFiles.remove(filesetFile)

        assert len(goldenFiles) == 0, \
               "ERROR: Fileset is missing files"
        
        testFilesetA.delete()
        testFileA.delete()
        testFileB.delete()
        testFileC.delete()

    def testFileCreateTransaction(self):
        """
        _testFileCreateTransaction_

        Create several files and add them to a fileset.  Commit the fileset
        and the files to the database, verifying that they can loaded back
        from the database.  Rollback the transaction to the point after the
        fileset has been created buy before the files have been associated with
        the filset.  Load the filesets from the database again and verify that
        they do not have any files.
        """
        testFileA = File(lfn = "/this/is/a/lfnA", size = 1024,
                         events = 20, cksum = 3)
        testFileA.addRun(Run( 1, *[45]))
        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024,
                         events = 20, cksum = 3)
        testFileB.addRun(Run( 1, *[45]))
        testFileC = File(lfn = "/this/is/a/lfnC", size = 1024,
                         events = 20, cksum = 3)
        testFileC.addRun(Run( 1, *[45]))
        testFileC.create()

        testFilesetA = Fileset(name = "TestFileset")
        testFilesetA.create()

        myThread = threading.currentThread()
        myThread.transaction.begin()

        testFilesetA.addFile(testFileA)
        testFilesetA.addFile(testFileB)
        testFilesetA.addFile(testFileC)
        testFilesetA.commit()

        testFilesetB = Fileset(name = testFilesetA.name)
        testFilesetB.loadData()
        testFilesetC = Fileset(id = testFilesetA.id)
        testFilesetC.loadData()

        assert testFilesetB.id == testFilesetA.id, \
               "ERROR: Load from name didn't load id"

        assert testFilesetC.name == testFilesetA.name, \
               "ERROR: Load from id didn't load name"

        goldenFiles = [testFileA, testFileB, testFileC]
        for filesetFile in testFilesetB.files:
            assert filesetFile in goldenFiles, \
                   "ERROR: Unknown file in fileset"
            goldenFiles.remove(filesetFile)

        assert len(goldenFiles) == 0, \
               "ERROR: Fileset is missing files"

        goldenFiles = [testFileA, testFileB, testFileC]
        for filesetFile in testFilesetC.files:
            assert filesetFile in goldenFiles, \
                   "ERROR: Unknown file in fileset"
            goldenFiles.remove(filesetFile)

        assert len(goldenFiles) == 0, \
               "ERROR: Fileset is missing files"

        myThread.transaction.rollback()

        testFilesetB.load()
        testFilesetC.load()

        assert len(testFilesetB.files) == 0, \
               "ERROR: Fileset B has too many files"

        assert len(testFilesetC.files) == 0, \
               "ERROR: Fileset C has too many files"        
        
        testFilesetA.delete()
        testFileA.delete()
        testFileB.delete()
        testFileC.delete()
            
if __name__ == "__main__":
        unittest.main()
