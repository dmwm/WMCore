#!/usr/bin/env python
""" 
Testcase for Fileset

Instantiate a Fileset, with an initial file on its Set. After being populated with 1000 random files,
its access methods and additional file insert methods are tested

"""

__revision__ = "$Id: Fileset_t.py,v 1.3 2008/11/25 15:53:17 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

import unittest
import logging
import random
import os
import threading

from WMCore.WMFactory import WMFactory
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset

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

    def testLoad(self):
        """
        _testLoad_

        Create a fileset and load it from the database using the two
        load methods.
        """
        testFileA = File(lfn = "/this/is/a/lfnA", size = 1024,
                         events = 20, run = 1, lumi = 45)
        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024,
                         events = 20, run = 1, lumi = 45)
        testFileC = File(lfn = "/this/is/a/lfnC", size = 1024,
                         events = 20, run = 1, lumi = 45)
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
        testFilesetB.load(method = "Fileset.LoadFromName")        
        testFilesetC = Fileset(id = testFilesetA.id)
        testFilesetC.load(method = "Fileset.LoadFromID")

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
                         events = 20, run = 1, lumi = 45)
        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024,
                         events = 20, run = 1, lumi = 45)
        testFileC = File(lfn = "/this/is/a/lfnC", size = 1024,
                         events = 20, run = 1, lumi = 45)
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
                         events = 20, run = 1, lumi = 45)
        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024,
                         events = 20, run = 1, lumi = 45)
        testFileC = File(lfn = "/this/is/a/lfnC", size = 1024,
                         events = 20, run = 1, lumi = 45)
        testFileB.create()

        testFilesetA = Fileset(name = "TestFileset")
        testFilesetA.create()

        testFilesetA.addFile(testFileA)
        testFilesetA.addFile(testFileB)
        testFilesetA.addFile(testFileC)
        testFilesetA.commit()

        testFilesetB = Fileset(name = testFilesetA.name)
        testFilesetB.load(method = "Fileset.LoadFromName")        
        testFilesetC = Fileset(id = testFilesetA.id)
        testFilesetC.load(method = "Fileset.LoadFromID")

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
        
    # test parentage
    
if __name__ == "__main__":
        unittest.main()
