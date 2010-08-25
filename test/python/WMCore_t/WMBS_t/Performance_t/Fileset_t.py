#!/usr/bin/env python

#First test with doing from scratch a class that just inherits from mnorman's WMBSBase
#Updated to use Fileset class wrapper to DAO objects
#-mnorman

"""
__FilesetTest__

Performance testcase for WMBS Fileset class

This class is abstract, proceed to the DB specific testcase
to run the test


"""
import unittest
import logging
import os
import commands
import threading
import random
import time


from sets import Set
from WMCore.WMFactory import WMFactory
from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.WMBSBase import WMBSBase
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.File import File


class FilesetTest(TestCase, WMBSBase):
    """
    __FilesetTest__

     Performance testcase for WMBS Fileset class

    """
    def runTest(self):
        """
        _runTest_

        Run all the unit tests.
        """
        unittest.main()

    def setUp(self):
        """
            Common setUp for Fileset object DAO tests
            
        """                
        #self.logger = logging.getLogger(logarg + 'FilesetPerformanceTest')
        
        #dbf = DBFactory(self.logger, sqlURI)

        #Call superclass setUp method
        WMBSBase.setUp(self)
        return

    def tearDown(self):
        """
            Common tearDown for Fileset object DAO tests
            
        """
        #Call superclass tearDown method
        WMBSBase.tearDown(self)
        return

    def testNew(self, times=1):
        """
            Testcase for the Fileset.New DAO class
            
        """
        print "testNew"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes
        
        for i in range(times):
            startTime = time.time()    
            testFileset = Fileset(name = "TestFileset"+str(i))
            testFileset.create()
            endTime = time.time()
            elapsedTime = endTime - startTime   
            self.totaltime = self.totaltime + elapsedTime     
            assert self.totaltime <= self.totalthreshold, 'New DAO '+ \
            'class - Operation too slow ( '+str(i+1)+' times, total elapsed'+ \
            ' time:'+str(self.totaltime)+', threshold:'+ \
            str(self.totalthreshold)+' )'

    def testDelete(self, times=1):
        """
            Testcase for the Fileset.Delete DAO class
            
        """
        print "testDelete"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        list = self.genFileset(number=times)

        for i in range(times):
            startTime = time.time()    
            testFileset = Fileset(name = list[i].name)
            testFileset.delete()
            endTime = time.time()
            elapsedTime = endTime - startTime   
            self.totaltime = self.totaltime + elapsedTime                        
            assert self.totaltime <= self.totalthreshold, 'Delete DAO '+ \
            'class - Operation too slow ( '+str(i+1)+' times, total elapsed'+ \
            ' time:'+str(self.totaltime)+', threshold:'+ \
            str(self.totalthreshold)+' )'

    def testExists(self, times=1):
        """
            Testcase for the Fileset.Exists DAO class
            
        """
        print "testExists"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        list = self.genFileset(number=times)

        for i in range(times):
            startTime = time.time()    
            testFileset = Fileset(name = list[i].name)
            testFileset.exists()
            endTime = time.time()
            elapsedTime = endTime - startTime   
            self.totaltime = self.totaltime + elapsedTime                        
            assert self.totaltime <= self.totalthreshold, 'Exists DAO '+ \
            'class - Operation too slow ( '+str(i+1)+' times, total elapsed'+ \
            ' time:'+str(self.totaltime)+', threshold:'+ \
            str(self.totalthreshold)+' )'

        return

    def testLoadFromID(self, times=1):
        """
            Testcase for the Fileset.LoadFromID DAO class
            
        """
        print "testLoadFromID"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        list = self.genFileset(number=times)

        for i in range(times):
            startTime = time.time()    
            testFileset = Fileset(id = list[i].id)
            testFileset.load()
            endTime = time.time()
            elapsedTime = endTime - startTime   
            self.totaltime = self.totaltime + elapsedTime     
            assert self.totaltime <= self.totalthreshold, 'LoadFromID DAO '+ \
            'class - Operation too slow ( '+str(i+1)+' times, total elapsed'+ \
            ' time:'+str(self.totaltime)+', threshold:'+ \
            str(self.totalthreshold)+' )'

        return

    def testLoadFromName(self, times=1):
        """
            Testcase for the Fileset.LoadFromName DAO class
            
        """
        print "testLoadFromName"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        list = self.genFileset(number=times)

        for i in range(times):
            startTime = time.time()    
            testFileset = Fileset(name = list[i].name)
            testFileset.load()
            endTime = time.time()
            elapsedTime = endTime - startTime   
            self.totaltime = self.totaltime + elapsedTime     
            assert self.totaltime <= self.totalthreshold, 'LoadFromID DAO '+ \
            'class - Operation too slow ( '+str(i+1)+' times, total elapsed'+ \
            ' time:'+str(self.totaltime)+', threshold:'+ \
            str(self.totalthreshold)+' )'

        return


    def testAddFile(self, times=1):
        """
            Testcase for the Fileset.addFile DAO class
            
        """
        print "testAddFile"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        list     = self.genFileset(number=times)
        fileList = self.genFiles(number=times)

        for i in range(times):
            startTime = time.time()    
            testFileset = Fileset(name = list[i].name)
            testFile    = File(lfn = fileList[i]['lfn'], size = fileList[i]['size'], events = fileList[i]['events'],
                               cksum = fileList[i]['cksum'])
            testFileset.addFile(testFile)
            endTime = time.time()
            elapsedTime = endTime - startTime   
            self.totaltime = self.totaltime + elapsedTime                        
            assert self.totaltime <= self.totalthreshold, 'Exists DAO '+ \
            'class - Operation too slow ( '+str(i+1)+' times, total elapsed'+ \
            ' time:'+str(self.totaltime)+', threshold:'+ \
            str(self.totalthreshold)+' )'

        return


    def testAddFileAndCommit(self, times=1):
        """
            Testcase for the Fileset.addFile and commit DAO class
            
        """
        print "testAddFile"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        list     = self.genFileset(number=times)
        fileList = self.genFiles(number=times)

        for i in range(times):
            startTime = time.time()    
            testFileset = Fileset(name = list[i].name)
            testFile    = File(lfn = fileList[i]['lfn'], size = fileList[i]['size'], events = fileList[i]['events'],
                               cksum = fileList[i]['cksum'])
            testFileset.addFile(testFile)
            testFileset.commit()
            endTime = time.time()
            elapsedTime = endTime - startTime   
            self.totaltime = self.totaltime + elapsedTime                        
            assert self.totaltime <= self.totalthreshold, 'Exists DAO '+ \
            'class - Operation too slow ( '+str(i+1)+' times, total elapsed'+ \
            ' time:'+str(self.totaltime)+', threshold:'+ \
            str(self.totalthreshold)+' )'

        return



    #Waiting for fileset parentage to be needed 

#    def testParentage(self):
#        print "testParentage"

#        childname = "ChildFileset1234"

#        childFileset = Fileset(name=childname,                         
#                        logger=self.logger, 
#                        dbfactory=self.dbf) 
#        childFileset.create()

        #Add the child fileset to the DB
        #self.dao(classname='Fileset.New').execute(name=childname)


if __name__ == "__main__":
    unittest.main()
        
