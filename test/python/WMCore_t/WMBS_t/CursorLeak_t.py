#!/usr/bin/env python
"""
_File_t_

Unit tests for the WMBS File class.
"""




import unittest
import logging
import os
import commands
import threading
import random

from WMCore.Database.DBCore import DBInterface
from WMCore.Database.DBFactory import DBFactory
from WMCore.DAOFactory import DAOFactory
from WMCore.WMBS.File import File
from WMCore.WMFactory import WMFactory
from WMQuality.TestInit import TestInit
from WMCore.DataStructs.Run import Run
from WMCore.Services.UUID import makeUUID

import nose
class CursorLeakTest(unittest.TestCase):

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
            
    def testCursor(self):
        """
        _testCursor_
        
        test the cursor closing is really affected
        
        create 100 files with 5 parents and  loop 100 times.
        If the cursors are exhausted will crash.?
        
        TODO: improve for more effective testing. 

        """
        
        raise nose.SkipTest
        fileList = []
        parentFile = None
        for i in range(100):
            testFile = File(lfn = "/this/is/a/lfn%s" % i, size = 1024, events = 10,
                            checksums = {"cksum": "1"})
            testFile.addRun(Run(1, *[i]))
            testFile.create()
            
            for j in range(5):
                parentFile = File(lfn = "/this/is/a/lfnP%s" % j, size = 1024,
                                  events = 10, checksums = {"cksum": "1"})
                parentFile.addRun(Run(1, *[j]))
                parentFile.create()
                testFile.addParent(parentFile['lfn'])
    
            fileList.append(testFile)
            
        for i in range(100):
            for file in fileList:
                file.loadData()
                file.getAncestors(level = 2)
                file.getAncestors(level = 2, type = "lfn")
        
        return
    
    def testLotsOfAncestors(self):
        """
        _testLotsOfAncestors_

        Create a file with 15 parents with each parent having 100 parents to
        verify that the query to return grandparents works correctly.
        """
        raise nose.SkipTest
        testFileA = File(lfn = "/this/is/a/lfnA", size = 1024, events = 10,
                        checksums = {"cksum": "1"}, locations = "se1.fnal.gov")
        testFileA.create()

        for i in xrange(15):
            testParent = File(lfn = makeUUID(), size = 1024, events = 10,
                              checksums = {"cksum": "1"}, locations = "se1.fnal.gov")
            testParent.create()
            testFileA.addParent(testParent["lfn"])

            for i in xrange(100):
                testGParent = File(lfn = makeUUID(), size = 1024, events = 10,
                                   checksums = {"cksum": "1"}, locations = "se1.fnal.gov")
                testGParent.create()
                testParent.addParent(testGParent["lfn"])                

        assert len(testFileA.getAncestors(level = 2, type = "lfn")) == 1500, \
               "ERROR: Incorrect grand parents returned"
        
        return
if __name__ == "__main__":
    unittest.main() 
