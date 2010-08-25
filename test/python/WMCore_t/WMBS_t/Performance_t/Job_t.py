#!/usr/bin/env python

import logging
import unittest
import time
from WMCore_t.WMBS_t.Performance_t.WMBSBase import WMBSBase
from WMCore.Database.DBFactory import DBFactory
from WMCore.WMBS.Job import Job
from WMCore.WMBS.File import File
from WMCore.DataStructs.Run import Run

class JobTest(unittest.TestCase, WMBSBase):
    """
    __JobTest__

     Performance testcase for WMBS Job class

     This class is abstract, proceed to the DB specific testcase
     to run the test


    """
    
    def setUp(self):
        #Call common setUp method from WMBSBase
        
        WMBSBase.setUp(self)
        return

    def tearDown(self):
        #Call superclass tearDown method
        WMBSBase.tearDown(self)
        return


    def testNew(self, times=1):         
        print "testNew"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times=self.testtimes

        jobgroup = self.genJobGroup(number=1)[0]
        jobgroup.create()
        joblist = self.genJobObjects(number=times, name='JobsNew')
        
        for i in range(times):
            startTime = time.time()    
            testJob = Job(name = joblist[i]['name'])
            testJob.create(group = jobgroup)
            endTime = time.time()
            elapsedTime = endTime - startTime
            self.totaltime = self.totaltime + elapsedTime  
            assert self.totaltime <= self.totalthreshold, 'New DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'

        return


    def testExists(self, times=1):         
        print "testExists"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times=self.testtimes

        jobs = self.genJob(number=times, name='JobsNew')
        
        for i in range(times):
            startTime = time.time()    
            testJob = Job(name = jobs[i]['name'])
            testJob.exists()
            endTime = time.time()
            elapsedTime = endTime - startTime
            self.totaltime = self.totaltime + elapsedTime  
            assert self.totaltime <= self.totalthreshold, 'New DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'

        return


    def testDelete(self, times=1):         
        print "testDelete"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times=self.testtimes

        jobs = self.genJob(number=times, name='JobsNew')
        
        for i in range(times):
            startTime = time.time()    
            testJob = Job(name = jobs[i]['name'])
            testJob.delete()
            endTime = time.time()
            elapsedTime = endTime - startTime
            self.totaltime = self.totaltime + elapsedTime  
            assert self.totaltime <= self.totalthreshold, 'New DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'

        return

    def testLoadByID(self, times=1):         
        print "testLoadByID"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times=self.testtimes

        jobs = self.genJob(number=times)

        for i in range(times):
            startTime = time.time()    
            testJob = Job(id = jobs[i]['id'])
            testJob.load()
            endTime = time.time()
            elapsedTime = endTime - startTime
            self.totaltime = self.totaltime + elapsedTime  
            assert self.totaltime <= self.totalthreshold, 'Load DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'

        return

    def testLoadByName(self, times=1):         
        print "testLoadByName"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times=self.testtimes

        jobs = self.genJob(number=times)

        for i in range(times):
            startTime = time.time()    
            testJob = Job(name = jobs[i]['name'])
            testJob.load()
            endTime = time.time()
            elapsedTime = endTime - startTime
            self.totaltime = self.totaltime + elapsedTime  
            assert self.totaltime <= self.totalthreshold, 'Load DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'

        return

#    def testAddOutput(self, times=1):         
#        print "testAddOutput"
#
#        #If testtimes is not set, the arguments are used for how many times
#        #the test method will be run
#        if self.testtimes != 0:
#            times=self.testtimes
#
#        joblist = self.genJobObjects(number=times, name='JobsNew')
#        files = self.genFiles(number=times, name="testAddFiles")
#        testFile = File(lfn = "/this/is/a/lfnQ", size = 1024, events = 10)
#        testFile.create()
#        testJobGroup = self.genJobGroup(number=1)[0]
#        testJobGroup.create()
#
#        testFileA = File(lfn = "/this/is/a/lfnA", size = 1024, events = 10)
#        testFileA.addRun(Run(1, *[45]))
#        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024, events = 10)
#        testFileB.addRun(Run(1, *[46]))
#        testFileA.create()
#        testFileB.create()
#
#        for i in range(times):
#            startTime = time.time()    
#            testJob = Job(name = joblist[i]['name'], files = [testFileA, testFileB])
#            testJob.create(group = testJobGroup)
#            testJob.addOutput(testFile)
#            endTime = time.time()
#            elapsedTime = endTime - startTime
#            self.totaltime = self.totaltime + elapsedTime  
#            assert self.totaltime <= self.totalthreshold, 'AddFiles DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'
#
#        return

    def testGetFiles(self, times=1):         
        print "testLoadByName"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times=self.testtimes

        jobs = self.genJob(number=times)

        for i in range(times):
            startTime = time.time()    
            testJob = Job(name = jobs[i]['name'])
            testJob.getFiles()
            endTime = time.time()
            elapsedTime = endTime - startTime
            self.totaltime = self.totaltime + elapsedTime  
            assert self.totaltime <= self.totalthreshold, 'Load DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'

        return

#    def testChangeStatus(self, times=1):         
#        print "testChangeStatus"
#
#        #If testtimes is not set, the arguments are used for how many times
#        #the test method will be run
#        if self.testtimes != 0:
#            times=self.testtimes
#
#        jobs = self.genJobObjects(number=times, name='JobsNew')
#        testJobGroup = self.genJobGroup(number=1)[0]
#        testJobGroup.create()
#        
#        for i in range(times):
#            startTime = time.time()    
#            testJob = Job(name = jobs[i]['name'])
#            testJob.create(group = testJobGroup)
#            testJob.changeStatus("ACTIVE")
#            endTime = time.time()
#            elapsedTime = endTime - startTime
#            self.totaltime = self.totaltime + elapsedTime 
#
#        return


    

            





# Deprecated apparently.  Functions are no longer in the Job.py wrapper
# -mnorman

#    def testActive(self, times=1):         
#        print "testActive"
#
#        #If testtimes is not set, the arguments are used for how many times
#        #the test method will be run
#        if self.testtimes != 0:
#            times=self.testtimes
#    
#        jobs = self.genJob(number=times)
#
#        for i in range(times):             
#            time = self.perfTest(dao=self.dao, action='Jobs.Active', job=jobs[i].id)
#            assert self.totaltime <= self.totalthreshold, 'Active DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'
#
#    def testComplete(self, times=1):         
#        print "testComplete"
#
#        #If testtimes is not set, the arguments are used for how many times
#        #the test method will be run
#        if self.testtimes != 0:
#            times=self.testtimes
#
#        jobs = self.genJob(number=times)
#
#        for i in range(times):             
#            time = self.perfTest(dao=self.dao, action='Jobs.Complete', job=jobs[i].id)
#            assert self.totaltime <= self.totalthreshold, 'Complete DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'
#
#    def testFailed(self, times=1):         
#        print "testFailed"
#
#        #If testtimes is not set, the arguments are used for how many times
#        #the test method will be run
#        if self.testtimes != 0:
#            times=self.testtimes
#
#        jobs = self.genJob(number=times)
#
#        for i in range(times):             
#            time = self.perfTest(dao=self.dao, action='Jobs.Failed', job=jobs[i].id)
#            assert self.totaltime <= self.totalthreshold, 'Failed DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'




# This function now appears to only be called once submitted.  Not sure I want to do that
# -mnorman

#    def testUpdateName(self, times=1):         
#        print "testUpdateName"
#
#        #If testtimes is not set, the arguments are used for how many times
#        #the test method will be run
#        if self.testtimes != 0:
#            times=self.testtimes
#
#        jobs = self.genJob(number=times)
#
#        for i in range(times):             
#            time = self.perfTest(dao=self.dao, action='Jobs.UpdateName', id=jobs[i].id, name="NewJobName"+str(i))
#            assert self.totaltime <= self.totalthreshold, 'UpdateName DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'



if __name__ == "__main__":
    unittest.main()
