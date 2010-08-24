#!/usr/bin/env python

import logging
from WMCore_t.WMBS_t.Performance_t.WMBSBase import WMBSBase
from WMCore.Database.DBFactory import DBFactory

class JobTest(WMBSBase):
    """
    __JobTest__

     Performance testcase for WMBS Job class

     This class is abstract, proceed to the DB specific testcase
     to run the test


    """
    
    def setUp(self, sqlURI='', logarg=''):
        #Call common setUp method from WMBSBase
                
        self.logger = logging.getLogger(logarg + 'JobPerformanceTest')
                    
        dbf = DBFactory(self.logger, sqlURI)
        
        WMBSBase.setUp(self,dbf=dbf)

        #Type the number of times you want the tests to be run
#        self.testtimes = 0

    def tearDown(self):
        #Call superclass tearDown method
        WMBSBase.tearDown(self)

    def testNew(self, times=1):         
        print "testNew"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times=self.testtimes

        jobgroup = self.genJobGroup(number=1)[0]
        joblist = self.genJobObjects(number=times, name='JobsNew')
        for i in range(times):             
            time = self.perfTest(dao=self.dao, action='Jobs.New', jobgroup=jobgroup.id, name=joblist[i].name)
            assert self.totaltime <= self.totalthreshold, 'New DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'

    def testActive(self, times=1):         
        print "testActive"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times=self.testtimes
    
        jobs = self.genJob(number=times)

        for i in range(times):             
            time = self.perfTest(dao=self.dao, action='Jobs.Active', job=jobs[i].id)
            assert self.totaltime <= self.totalthreshold, 'Active DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'

    def testComplete(self, times=1):         
        print "testComplete"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times=self.testtimes

        jobs = self.genJob(number=times)

        for i in range(times):             
            time = self.perfTest(dao=self.dao, action='Jobs.Complete', job=jobs[i].id)
            assert self.totaltime <= self.totalthreshold, 'Complete DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'

    def testFailed(self, times=1):         
        print "testFailed"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times=self.testtimes

        jobs = self.genJob(number=times)

        for i in range(times):             
            time = self.perfTest(dao=self.dao, action='Jobs.Failed', job=jobs[i].id)
            assert self.totaltime <= self.totalthreshold, 'Failed DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'

    def testLoad(self, times=1):         
        print "testLoad"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times=self.testtimes

        jobs = self.genJob(number=times)

        for i in range(times):             
            time = self.perfTest(dao=self.dao, action='Jobs.Load', id=jobs[i].id)
            assert self.totaltime <= self.totalthreshold, 'Load DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'

    def testClearStatus(self, times=1):         
        print "testClearStatus"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times=self.testtimes

        jobs = self.genJob(number=times)

        for i in range(times):             
            time = self.perfTest(dao=self.dao, action='Jobs.ClearStatus', job=jobs[i].id)
            assert self.totaltime <= self.totalthreshold, 'ClearStatus DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'

    def testUpdateName(self, times=1):         
        print "testUpdateName"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times=self.testtimes

        jobs = self.genJob(number=times)

        for i in range(times):             
            time = self.perfTest(dao=self.dao, action='Jobs.UpdateName', id=jobs[i].id, name="NewJobName"+str(i))
            assert self.totaltime <= self.totalthreshold, 'UpdateName DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'

    def testAddFiles(self, times=1):         
        print "testAddFiles"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times=self.testtimes

        jobs = self.genJob(number=times)
        files = self.genFiles(number=times, name="testAddFiles")

        for i in range(times):             
            time = self.perfTest(dao=self.dao, action='Jobs.AddFiles', id=jobs[i].id, file=files[i]["id"])
            assert self.totaltime <= self.totalthreshold, 'AddFiles DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'
