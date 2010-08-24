#!/usr/bin/env python

import logging, random
import time
from WMCore_t.WMBS_t.Performance_t.WMBSBase import WMBSBase
from WMCore.WMBS.File import File
from WMCore.Database.DBFactory import DBFactory

class FileTest(WMBSBase):
    """
    __FileTest__

     Performance testcase for WMBS File class

     This class is abstract, proceed to the DB specific testcase
     to run the test


    """
    
    def setUp(self, sqlURI='', logarg=''):
        #Call common setUp method from BaseTest

        self.totaltime = 0
                
        self.logger = logging.getLogger(logarg + 'FilePerformanceTest')
        
        dbf = DBFactory(self.logger, sqlURI)
        
        WMBSBase.setUp(self,dbf=dbf)

        #Type the number of times you want the tests to be run
#        self.testtimes = 0

    def tearDown(self):
        #Call superclass tearDown method
        WMBSBase.tearDown(self)

    def testAdd(self, times=1):
        print "testAdd"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times=self.testtimes

        list = self.genFileObjects(number=times)        
        for i in range(times):
            time = self.perfTest(dao=self.dao, action='Files.Add', files=str(list[i]['lfn']), size=list[i]['size'], events=list[i]['events'])
            self.totaltime = self.totaltime + time                        
            assert self.totaltime <= self.totalthreshold, 'Add DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'

    def testAddRunLumi(self, times=1): 
        print "testAddRunLumi"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times=self.testtimes

        list = self.genFileObjects(number=times)

        for i in range(times):
            time = self.perfTest(dao=self.dao, action='Files.AddRunLumi', files=str(list[i]['lfn']), run=list[i]['run'], lumi=list[i]['lumi'])
            self.totaltime = self.totaltime + time                        
            assert self.totaltime <= self.totalthreshold, 'AddRunLumi DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'

    def testAddToFileset(self, times=1):
        print "testAddToFileset"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times=self.testtimes

        list = self.genFiles(number=times, name="TestNew")

        for i in range(times):
            time = self.perfTest(dao=self.dao, action='Files.AddToFileset', file=str(list[i]['lfn']), fileset="TestNewFiles")
            self.totaltime = self.totaltime + time
            assert self.totaltime <= self.totalthreshold, 'AddToFileset DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'

    def testDelete(self, times=1):
        print "testDelete"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times=self.testtimes

        list = self.genFiles(number=times)
        for i in range(times):       
            time = self.perfTest(dao=self.dao, action='Files.Delete', file=list[i]['lfn'])
            self.totaltime = self.totaltime + time            
            assert self.totaltime <= self.totalthreshold, 'Delete DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'

    def testGetByID(self, times=1):
        print "testGetByID"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times=self.testtimes

        list = self.genFiles(number=1)

        for i in range(times):
           time = self.perfTest(dao=self.dao, action='Files.GetByID', files=list[0]["id"])
           self.totaltime = self.totaltime + time                        
           assert self.totaltime <= self.totalthreshold, 'GetByID DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'

    def testGetByLFN(self, times=1):
        print "testGetByLFN"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times=self.testtimes

        list = self.genFiles(number=1)

        for i in range(times):
            time = self.perfTest(dao=self.dao, action='Files.GetByLFN', files=list[0]['lfn'])
            self.totaltime = self.totaltime + time                        
            assert self.totaltime <= self.totalthreshold, 'GetByLFN DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'

    def testGetLocation(self, times=1):
        print "testGetLocation"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times=self.testtimes

        list = self.genFiles(number=1)

        for i in range(times):        
            time = self.perfTest(dao=self.dao, action='Files.GetLocation', file=list[0]['lfn'])
            self.totaltime = self.totaltime + time                        
            assert self.totaltime <= self.totalthreshold, 'GetLocation DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'

    def testGetParents(self, times=1):
        print "testGetParents"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times=self.testtimes

        list = self.genFiles(number=1)

        for i in range(times):        
            time = self.perfTest(dao=self.dao, action='Files.GetParents', files=list[0]['lfn'])
            self.totaltime = self.totaltime + time                        
            assert self.totaltime <= self.totalthreshold, 'GetParents DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'
                
#    def testHeritage(self):
#        self.dao(classname='Files.Heritage')                
#        #TODO - parent and child argument settings
#       self.perfTest(dao=self.dao, execinput='parent= , child= '+self.baseexec)
#        assert time <= self.threshold, 'Heritage DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testInFileset(self, times=1):
        print "testInFileset"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times=self.testtimes

        list = self.genFileset(number=1)

        for i in range(times):        
            time = self.perfTest(dao=self.dao, action='Files.InFileset', fileset="TestFileset")
            self.totaltime = self.totaltime + time                        
            assert self.totaltime <= self.totalthreshold, 'InFileset DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'

    def testSetLocation(self, times=1):
        print "testSetLocation"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times=self.testtimes

        list = self.genLocation(number=1, name='SetLocation')

        file = self.genFiles(number=times)        

        for i in range(times):
            time = self.perfTest(dao=self.dao, action='Files.SetLocation', file=file[i]["lfn"], sename=list[0]) 
            self.totaltime = self.totaltime + time            
            assert self.totaltime <= self.totalthreshold, 'SetLocation DAO \
           class - Operation too slow ( '+str(i+1)+' times, total elapsed \
           time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+'\
            )'


if __name__ == "__main__":
    unittest.main()
