
#!/usr/bin/env python

import logging
import unittest
import time
from WMCore_t.WMBS_t.Performance_t.WMBSBase import WMBSBase
from WMCore.Database.DBFactory import DBFactory
from WMCore.WMBS.Job import Job
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.File import File
from WMCore.DataStructs.Run import Run
from WMCore.WMBS.Fileset import Fileset as WMBSFileset

from nose.plugins.attrib import attr
class JobGroupTest(unittest.TestCase, WMBSBase):
    __performance__=True
    """
    __JobGroupTest__

     Performance testcase for WMBS JobGroup class

     This class is abstract, proceed to the DB specific testcase
     to run the test


    """
    
    def setUp(self, sqlURI='', logarg=''):
        #Call common setUp method from WMBSBase
                
        
        WMBSBase.setUp(self)

        #Type the number of times you want the tests to be run
#        self.testtimes = 0

    def tearDown(self):
        #Call superclass tearDown method
        WMBSBase.tearDown(self)

    def createTestJobGroup(self, num, commitFlag = True):
        """
        _createTestJobGroup_
        
               with testSubscription 
                     using testWorkflow (wf001) and testWMBSFilset
        add testJobA with testFileA and testJobB with testFileB
            to testJobGroup
        return testJobGroup
        """

        #Shamelessly stolen from JobGroup_t.py
        
        testWorkflow = Workflow(spec = 'spec%s.xml'%(str(num)), owner = "Simon",
                                name = "wf001"+str(num), task = 'test%i'%num)
        testWorkflow.create()
        
        testWMBSFileset = WMBSFileset(name = "TestFileset")
        testWMBSFileset.create()
        
        testSubscription = Subscription(fileset = testWMBSFileset,
                                        workflow = testWorkflow)
        testSubscription.create()

        testJobGroupA = JobGroup(subscription = testSubscription)
        testJobGroupA.create()

        testFileA = File(lfn = "/this/is/a/lfnA"+str(num), size = 1024, events = 10)
        testFileA.addRun(Run(10, *[12312]))

        testFileB = File(lfn = "/this/is/a/lfnB"+str(num), size = 1024, events = 10)
        testFileB.addRun(Run(10, *[12312]))
        testFileA.create()
        testFileB.create()

        testJobA = Job(name = "TestJobA"+str(num))
        testJobA.addFile(testFileA)
        
        testJobB = Job(name = "TestJobB"+str(num))
        testJobB.addFile(testFileB)
        
        testJobGroupA.add(testJobA)
        testJobGroupA.add(testJobB)
        if commitFlag:
            testJobGroupA.commit()
        
        return testJobGroupA

    def createTestJobList(self, times):
        jobGroupList = []

        for i in range(times):
            sampleJobGroup = self.createTestJobGroup(i)
            jobGroupList.append(sampleJobGroup)

        return jobGroupList

    def testNew(self, times=1):         
        print "testNew"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times=self.testtimes

        subscription=self.genSubscription(number=times, name='testNew')
        output = self.genFileset(number=1, name='testNew')[0]

        for i in range(times):
            startTime = time.time()
            testJobGroup = JobGroup(subscription = subscription[i])
            testJobGroup.create()
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

        subscription=self.genSubscription(number=times, name='testNew')
        output = self.genFileset(number=1, name='testNew')[0]

        for i in range(times):
            startTime = time.time()
            testJobGroup = JobGroup(subscription = subscription[i])
            testJobGroup.exists()
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

        subscription=self.genSubscription(number=times, name='testNew')

        for i in range(times):
            startTime = time.time()
            testJobGroup = JobGroup(subscription = subscription[i])
            testJobGroup.delete()
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

        jobGroupList = self.createTestJobList(times)

        for i in range(times):
            startTime = time.time()
            testJobGroup = JobGroup(id = jobGroupList[i].id)
            testJobGroup.load()
            endTime = time.time()
            elapsedTime = endTime - startTime
            self.totaltime = self.totaltime + elapsedTime
            assert self.totaltime <= self.totalthreshold, 'New DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'

        return


    def testLoadByUID(self, times=1):         
        print "testLoadByID"

        #If testtimes is not set, the arguments are used for how many times
        #the test method will be run
        if self.testtimes != 0:
            times=self.testtimes

        jobGroupList = self.createTestJobList(times)

        for i in range(times):
            startTime = time.time()
            testJobGroup = JobGroup(uid = jobGroupList[i].uid)
            testJobGroup.load()
            endTime = time.time()
            elapsedTime = endTime - startTime
            self.totaltime = self.totaltime + elapsedTime
            assert self.totaltime <= self.totalthreshold, 'New DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'

        return


#    def testGetJobIDs(self, times=1):         
#        print "testGetJobIDs"
#
#        #If testtimes is not set, the arguments are used for how many times
#        #the test method will be run
#        if self.testtimes != 0:
#            times=self.testtimes
#
#        jobGroupList = self.createTestJobList(times)
#
#        for i in range(times):
#            startTime = time.time()
#            testJobGroup = jobGroupList[i]
#            testJobGroup.getJobIDs()
#            endTime = time.time()
#            elapsedTime = endTime - startTime
#            self.totaltime = self.totaltime + elapsedTime
#            assert self.totaltime <= self.totalthreshold, 'New DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'
#
#        return


#    def testRecordAcquire(self, times=1):
#        """
#        Tests the recordAcquire function of JobGroup.py
#        
#        """
#        print "testRecordAcquire"
#
#    
#
#        #If testtimes is not set, the arguments are used for how many times
#        #the test method will be run
#        if self.testtimes != 0:
#            times=self.testtimes
#
#        jobGroupList = self.createTestJobList(times)
#
#        for i in range(times):
#            startTime = time.time()
#            testJobGroup = jobGroupList[i]
#            testJobGroup.recordAcquire()
#            endTime = time.time()
#            elapsedTime = endTime - startTime
#            self.totaltime = self.totaltime + elapsedTime
#            assert self.totaltime <= self.totalthreshold, 'New DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'
#
#        return


#    def testRecordComplete(self, times=1):
#        """
#        Tests the recordComplete function of JobGroup.py
#        
#        """
#        print "testRecordComplete"
#
#    
#
#        #If testtimes is not set, the arguments are used for how many times
#        #the test method will be run
#        if self.testtimes != 0:
#            times=self.testtimes
#
#        jobGroupList = self.createTestJobList(times)
#
#        for i in range(times):
#            startTime = time.time()
#            testJobGroup = jobGroupList[i]
#            testJobGroup.recordComplete()
#            endTime = time.time()
#            elapsedTime = endTime - startTime
#            self.totaltime = self.totaltime + elapsedTime
#            assert self.totaltime <= self.totalthreshold, 'New DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'
#
#        return


#    def testRecordFail(self, times=1):
#        """
#        Tests the recordFail function of JobGroup.py
#        
#        """
#        print "testRecordFail"
#
#    
#
#        #If testtimes is not set, the arguments are used for how many times
#        #the test method will be run
#        if self.testtimes != 0:
#            times=self.testtimes
#
#        jobGroupList = self.createTestJobList(times)
#
#        for i in range(times):
#            startTime = time.time()
#            testJobGroup = jobGroupList[i]
#            testJobGroup.recordFail()
#            endTime = time.time()
#            elapsedTime = endTime - startTime
#            self.totaltime = self.totaltime + elapsedTime
#            assert self.totaltime <= self.totalthreshold, 'New DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'
#
#        return


#    def testStatus(self, times=1):
#        """
#        Tests the recordFail function of JobGroup.py
#        
#        """
#        print "testRecordFail"
#
#    
#
#        #If testtimes is not set, the arguments are used for how many times
#        #the test method will be run
#        if self.testtimes != 0:
#            times=self.testtimes
#
#        jobGroupList = self.createTestJobList(times)
#
#        for i in range(times):
#            startTime = time.time()
#            testJobGroup = jobGroupList[i]
#            testJobGroup.status()
#            endTime = time.time()
#            elapsedTime = endTime - startTime
#            self.totaltime = self.totaltime + elapsedTime
#            assert self.totaltime <= self.totalthreshold, 'New DAO class - Operation too slow ( '+str(i+1)+' times, total elapsed time:'+str(self.totaltime)+', threshold:'+str(self.totalthreshold)+' )'
#
#        return


if __name__ == "__main__":
    unittest.main()
