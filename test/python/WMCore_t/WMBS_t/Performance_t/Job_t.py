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

    def tearDown(self):
        #Call superclass tearDown method
        WMBSBase.tearDown(self)

    def testNew(self):         
        print "testNew"

        testJobName = 'TestNewJob'

        time = self.perfTest(dao=self.dao, action='Jobs.New', jobgroup=self.testJobGroup.id, name=testJobName)
        assert time <= self.threshold, 'New DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testActive(self):         
        print "testActive"

        time = self.perfTest(dao=self.dao, action='Jobs.Active', job=self.testJob.id)
        assert time <= self.threshold, 'Active DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testComplete(self):         
        print "testComplete"

        time = self.perfTest(dao=self.dao, action='Jobs.Complete', job=self.testJob.id)
        assert time <= self.threshold, 'Complete DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testFailed(self):         
        print "testFailed"

        time = self.perfTest(dao=self.dao, action='Jobs.Failed', job=self.testJob.id)
        assert time <= self.threshold, 'Failed DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testLoad(self):         
        print "testLoad"

        time = self.perfTest(dao=self.dao, action='Jobs.Load', id=self.testJob.id)
        assert time <= self.threshold, 'Load DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testClearStatus(self):         
        print "testClearStatus"

        time = self.perfTest(dao=self.dao, action='Jobs.ClearStatus', job=self.testJob.id)
        assert time <= self.threshold, 'ClearStatus DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testUpdateName(self):         
        print "testUpdateName"

        time = self.perfTest(dao=self.dao, action='Jobs.UpdateName', id=self.testJob.id, name='NewJobName')
        assert time <= self.threshold, 'UpdateName DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testAddFiles(self):         
        print "testAddFiles"
        
        time = self.perfTest(dao=self.dao, action='Jobs.AddFiles', id=self.testJob.id, file=self.testFile["id"])
        assert time <= self.threshold, 'AddFiles DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'


