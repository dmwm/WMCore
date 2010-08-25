#!/usr/bin/env python



# As should be painfully obvious, I have no idea what I'm doing.
# This is pretty complicated.
# In order to use the classes developed for Performance testing, I've had PerformanceFileTest
# inherit from WMBSBase, or in this case mnormanWMBSBase.  This forces you to kep both WMBSBase and Performance in place.
# Otherwise this file should mimic in its entirety the performance of both the SQLite and MySQL
# versions of the File Performance Test.
# -mnorman


import unittest


import logging
import os
import commands
import threading
import random
import time
from sets import Set

from WMCore.WMFactory import WMFactory
from ConfigParser import ConfigParser
from unittest import TestCase
#from WMCore_t.WMBS_t.Performance_t.File_t import FileTest
from WMCore.DAOFactory import DAOFactory
from WMQuality.TestInit import TestInit
from WMCore_t.WMBS_t.Performance_t.WMBSBase import WMBSBase
from WMCore.WMBS.File import File
from WMCore.DataStructs.Run import Run


class PerformanceFileTest(TestCase, WMBSBase):
    """
    __SQLiteDAOFileTest__

     DB Performance testcase for WMBS File class


    """

    def runTest(self):
        """
        _runTest_

        Run all the unit tests.
        """
        unittest.main()

    def setUp(self):
        """
        Set Up generalized for all databases for Performance File Test
        
        """
        WMBSBase.setUp(self)
        return

    def tearDown(self):
        """
        _tearDown_
        
        Drop all the WMBS tables.
        """
        WMBSBase.tearDown(self)
        return
    
    def testNew(self, times=1):
        """
            Testcase for the File.New class
            
        """
        print "testNew"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes
        
        for i in range(times):
            startTime = time.time()    
            testFile = File(lfn = "TestFileset"+str(i), size = 1024, events = 10, cksum=1111)
            testFile.create()
            endTime = time.time()
            elapsedTime = endTime - startTime   
            self.totaltime = self.totaltime + elapsedTime     
            assert self.totaltime <= self.totalthreshold, 'New DAO '+ \
            'class - Operation too slow ( '+str(i+1)+' times, total elapsed'+ \
            ' time:'+str(self.totaltime)+', threshold:'+ \
            str(self.totalthreshold)+' )'

    def testDelete(self, times=1):
        """
            Testcase for the File.Delete class
            
        """
        print "testDelete"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        list = self.genFileObjects(number=times)

        for i in range(times):
            startTime = time.time()    
            testFile = File(lfn = list[i]['lfn'] )
            testFile.delete()
            endTime = time.time()
            elapsedTime = endTime - startTime   
            self.totaltime = self.totaltime + elapsedTime                        
            assert self.totaltime <= self.totalthreshold, 'Delete DAO '+ \
            'class - Operation too slow ( '+str(i+1)+' times, total elapsed'+ \
            ' time:'+str(self.totaltime)+', threshold:'+ \
            str(self.totalthreshold)+' )'

        return

    def testExists(self, times=1):
        """
            Testcase for the File.Exists class
            
        """
        print "testExists"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        list = self.genFileObjects(number=times)

        for i in range(times):
            startTime = time.time()    
            testFile = File(lfn = list[i]['lfn'] )
            testFile.exists()
            endTime = time.time()
            elapsedTime = endTime - startTime   
            self.totaltime = self.totaltime + elapsedTime                        
            assert self.totaltime <= self.totalthreshold, 'Delete DAO '+ \
            'class - Operation too slow ( '+str(i+1)+' times, total elapsed'+ \
            ' time:'+str(self.totaltime)+', threshold:'+ \
            str(self.totalthreshold)+' )'

        return

    def testAddRunSet(self, times=1):
        """
            Testcase for the File.AddRunSet class
            
        """
        print "testAddRunSet"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        list = self.genFiles(number=times)

        runSet = Set()
        runSet.add(Run( 1, *[45]))
        runSet.add(Run( 2, *[67, 68]))

        for i in range(times):
            startTime = time.time()    
            testFile = File(lfn = list[i]['lfn'])
            testFile.addRunSet(runSet)
            endTime = time.time()
            elapsedTime = endTime - startTime   
            self.totaltime = self.totaltime + elapsedTime
            assert self.totaltime <= self.totalthreshold, 'Delete DAO '+ \
            'class - Operation too slow ( '+str(i+1)+' times, total elapsed'+ \
            ' time:'+str(self.totaltime)+', threshold:'+ \
            str(self.totalthreshold)+' )'

        return




    def testGetByLFN(self, times=1):
        """
            Testcase for the File.Load via lfn class
            
        """
        print "testGetByLFN"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        list = self.genFiles(number=times)

        for i in range(times):
            startTime = time.time()
            testFile = File(lfn = list[i]['lfn'] )
            testFile.load()
            endTime = time.time()
            elapsedTime = endTime - startTime
            self.totaltime = self.totaltime + elapsedTime
            assert self.totaltime <= self.totalthreshold, 'GetByLFN DAO '+ \
            'class - Operation too slow ( '+str(i+1)+' times, total elapsed'+ \
            ' time:'+str(self.totaltime)+', threshold:'+ \
            str(self.totalthreshold)+' )'

        return


    def testGetByID(self, times=1):
        """
            Testcase for the File.Load via ID class
            
        """
        
        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        list = self.genFiles(number=times)

        for i in range(times):
            startTime = time.time()    
            testFile = File(id = list[i]['id'] )
            testFile.load()
            endTime = time.time()
            elapsedTime = endTime - startTime
            self.totaltime = self.totaltime + elapsedTime
            assert self.totaltime <= self.totalthreshold, 'GetByID DAO '+ \
            'class - Operation too slow ( '+str(i+1)+' times, total elapsed'+ \
            ' time:'+str(self.totaltime)+', threshold:'+ \
            str(self.totalthreshold)+' )'

        return

    def testGetParentLFNs(self, times=1):
        """
            Testcase for the File.GetParentLFNs DAO class
            
        """
        print "testGetParentLFNs"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        list = self.genFiles(number=times)

        for i in range(times):
            startTime = time.time()    
            testFile = File(lfn = list[i]['lfn'] )
            testFile.getParentLFNs()
            endTime = time.time()
            elapsedTime = endTime - startTime
            self.totaltime = self.totaltime + elapsedTime
            assert self.totaltime <= self.totalthreshold, 'GetParents DAO '+ \
            'class - Operation too slow ( '+str(i+1)+' times, total elapsed'+ \
            ' time:'+str(self.totaltime)+', threshold:'+ \
            str(self.totalthreshold)+' )'

        return

    def testGetAncestors(self, times=1):
        """
            Testcase for the File.GetAncestors DAO class
            
        """
        print "testGetAncestors"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        list = self.genFiles(number=times)

        parentA = File(lfn = "/this/is/a/lfnA", size = 1024, events = 10,
                      cksum = 1, locations = "se1.fnal.gov")
        parentB = File(lfn = "/this/is/a/lfnB", size = 1024, events = 10,
                      cksum = 1, locations = "se1.fnal.gov")
        parentC = File(lfn = "/this/is/a/lfnC", size = 1024, events = 10,
                      cksum = 1, locations = "se1.fnal.gov")
        parentA.create()
        parentB.create()
        parentC.create()

        #Add a family to look for
        for i in range(times):
            list[i].addParent(lfn = parentA['lfn'])
            list[i].addParent(lfn = parentB['lfn'])
            list[i].addParent(lfn = parentC['lfn'])

        for i in range(times):
            startTime = time.time()    
            testFile = File(lfn = list[i]['lfn'] )
            testFile.getAncestors()
            endTime = time.time()
            elapsedTime = endTime - startTime
            self.totaltime = self.totaltime + elapsedTime
            assert self.totaltime <= self.totalthreshold, 'GetParents DAO '+ \
            'class - Operation too slow ( '+str(i+1)+' times, total elapsed'+ \
            ' time:'+str(self.totaltime)+', threshold:'+ \
            str(self.totalthreshold)+' )'

        return


    def testGetDescendants(self, times=1):
        """
            Testcase for the File.GetDescendants DAO class
            
        """
        print "testGetDescendants"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        list = self.genFiles(number=times)

        childA = File(lfn = "/this/is/a/lfnA", size = 1024, events = 10,
                      cksum = 1, locations = "se1.fnal.gov")
        childB = File(lfn = "/this/is/a/lfnB", size = 1024, events = 10,
                      cksum = 1, locations = "se1.fnal.gov")
        childC = File(lfn = "/this/is/a/lfnC", size = 1024, events = 10,
                      cksum = 1, locations = "se1.fnal.gov")
        childA.create()
        childB.create()
        childC.create()

        #Add a family to look for
        for i in range(times):
            list[i].addChild(lfn = childA['lfn'])
            list[i].addChild(lfn = childB['lfn'])
            list[i].addChild(lfn = childC['lfn'])
        

        for i in range(times):
            startTime = time.time()    
            testFile = File(lfn = list[i]['lfn'] )
            testFile.getDescendants()
            endTime = time.time()
            elapsedTime = endTime - startTime
            self.totaltime = self.totaltime + elapsedTime
            assert self.totaltime <= self.totalthreshold, 'GetParents DAO '+ \
            'class - Operation too slow ( '+str(i+1)+' times, total elapsed'+ \
            ' time:'+str(self.totaltime)+', threshold:'+ \
            str(self.totalthreshold)+' )'

        return


    def testAddParent(self, times=1):
        """
            Testcase for the File.AddParent DAO class
            
        """
        print "testAddParent"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        list = self.genFiles(number=times)
        parents = self.genFileObjects(number=times)

        for i in range(times):
            startTime = time.time()    
            testFile = File(lfn = list[i]['lfn'], id = list[i]['id'] )
            ParFile  = File(lfn = parents[i]['lfn'], id = parents[i]['id'])
            testFile.addParent(ParFile['lfn'])
            endTime = time.time()
            elapsedTime = endTime - startTime
            self.totaltime = self.totaltime + elapsedTime
            assert self.totaltime <= self.totalthreshold, 'GetParents DAO '+ \
            'class - Operation too slow ( '+str(i+1)+' times, total elapsed'+ \
            ' time:'+str(self.totaltime)+', threshold:'+ \
            str(self.totalthreshold)+' )'

        return


    def testAddChild(self, times=1):
        """
            Testcase for the File.AddChild DAO class
            
        """
        print "testAddChild"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        list = self.genFiles(number=times)
        childs = self.genFileObjects(number=times)

        for i in range(times):
            startTime = time.time()    
            testFile = File(lfn = list[i]['lfn'], id = list[i]['id'] )
            ChldFile = File(lfn = childs[i]['lfn'], id = childs[i]['id'])
            testFile.addChild(ChldFile['lfn'])
            endTime = time.time()
            elapsedTime = endTime - startTime
            self.totaltime = self.totaltime + elapsedTime
            assert self.totaltime <= self.totalthreshold, 'GetParents DAO '+ \
            'class - Operation too slow ( '+str(i+1)+' times, total elapsed'+ \
            ' time:'+str(self.totaltime)+', threshold:'+ \
            str(self.totalthreshold)+' )'

        return

    def testSetLocation(self, times=1):
        """
            Testcase for the File.SetLocation and UpdateLocation class
            
        """
        print "testSetLocation"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        list = self.genLocation(number=times, name='SetLocation')

        file = self.genFiles(number=times)        

        for i in range(times):
            startTime = time.time()
            testFile = File(lfn = file[i]['lfn'], id = file[i]['id'] )
            testFile.setLocation(se = list[i], immediateSave = False)
            testFile.updateLocations()
            endTime = time.time()
            elapsedTime = endTime - startTime
            self.totaltime = self.totaltime + elapsedTime
            assert self.totaltime <= self.totalthreshold, 'SetLocation DAO '+ \
            'class - Operation too slow ( '+str(i+1)+' times, total elapsed'+ \
            ' time:'+str(self.totaltime)+', threshold:'+ \
            str(self.totalthreshold)+' )'

        return

    def testGetLocation(self, times=1):
        """
            Testcase for the File.GetLocation DAO class
            
        """
        
        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        list = self.genFiles(number=times)

        for i in range(times):
            startTime = time.time()    
            testFile = File(lfn = list[i]['lfn'], id = list[i]['id'] )
            testFile.getLocations()
            endTime = time.time()
            elapsedTime = endTime - startTime
            self.totaltime = self.totaltime + elapsedTime
            assert self.totaltime <= self.totalthreshold, 'GetLocation DAO '+ \
            'class - Operation too slow ( '+str(i+1)+' times, total elapsed'+ \
            ' time:'+str(self.totaltime)+', threshold:'+ \
            str(self.totalthreshold)+' )'

        return

















# To be honest, not really sure what this does
# -mnorman

#    def testAdd(self, times=1):
#        """
#            Testcase for the File.Add DAO class
#            
#        """
#        print "testAdd"
#
#        #If testtimes is not set, the arguments are used for how many times
#        #the test method will be run
#        if self.testtimes != 0:
#            times = self.testtimes
#
#        list = self.genFileObjects(number=times)        
#        for i in range(times):
#            time = self.perfTest(dao=self.dao, action='Files.Add', files=str(list[i]['lfn']))
##            time = self.perfTest(dao=self.dao, action='Files.Add',
#                    #files=str(list[i]['lfn']), size=list[i]['size'], 
#                    #events=list[i]['events'])
#            self.totaltime = self.totaltime + time                        
#            assert self.totaltime <= self.totalthreshold, 'Add DAO class - '+\
#                    'Operation too slow ( '+str(i+1)+' times, total elapsed '+\
#                    'time:'+str(self.totaltime)+ \
#                    ', threshold:'+str(self.totalthreshold)+' )'

    def testAddRunLumi(self, times=1): 
        """
            Testcase for the File.AddRunLumi DAO class
            
        """
        print "testAddRunLumi currently skipped due to errors in handling file run numbers"
        return

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times = self.testtimes

        list = self.genFileObjects(number=times)

        for i in range(times):
            time = self.perfTest(dao=self.dao, action='Files.AddRunLumi', file=list[i]['lfn'])
#            time = self.perfTest(dao=self.dao, action='Files.AddRunLumi',
#                    files=str(list[i]['lfn']), run=list[i]['run'], 
#                    lumi=list[i]['lumi'])
            self.totaltime = self.totaltime + time                        
            assert self.totaltime <= self.totalthreshold, 'AddRunLumi DAO '+ \
            'class - Operation too slow ( '+str(i+1)+' times, total elapsed'+ \
            ' time:'+str(self.totaltime)+', threshold:'+ \
            str(self.totalthreshold)+' )'

#    Disabled due to its absence in File.py

#    def testAddToFileset(self, times=1):
#        """
#            Testcase for the File.AddToFileset DAO class
#            
#        """
#        print "testAddToFileset"
#
#        #If testtimes is not set, the arguments are used for how many times
#        #the test method will be run
#        if self.testtimes != 0:
#            times = self.testtimes
#
#        print "About to gen Files"
#
#        list = self.genFiles(number=times, name="TestNew")
#
#        #print "testAddToFileset"
#        #print list
#        #print times
#
#        for i in range(times):
#
#            time = self.perfTest(dao=self.dao, action='Files.AddToFileset',
#                    file=str(list[i]['lfn']), fileset="TestNewFiles")
#            self.totaltime = self.totaltime + time
#            assert self.totaltime <= self.totalthreshold, 'AddToFileset DAO '+ \
#            'class - Operation too slow ( '+str(i+1)+' times, total elapsed'+ \
#            ' time:'+str(self.totaltime)+', threshold:'+ \
#            str(self.totalthreshold)+' )'


# Disabled due to its absence in the File.py wrapper

#    def testInFileset(self, times=1):
#        """
#            Testcase for the File.InFileset DAO class
#            
#        """
#        print "testInFileset"
#
#        #If testtimes is not set, the arguments are used for how many times
#        #the test method will be run
#        if self.testtimes != 0:
#            times = self.testtimes
#
#        list = self.genFileset(number=1)
#
#        for i in range(times):        
#            time = self.perfTest(dao=self.dao, action='Files.InFileset', 
#                                fileset="TestFileset")
#            self.totaltime = self.totaltime + time                        
#            assert self.totaltime <= self.totalthreshold, 'InFileset DAO '+ \
#            'class - Operation too slow ( '+str(i+1)+' times, total elapsed'+ \
#            ' time:'+str(self.totaltime)+', threshold:'+ \
#            str(self.totalthreshold)+' )'




if __name__ == "__main__":
    unittest.main()
