#!/usr/bin/env python

import logging
from WMCore_t.WMBS_t.Performance_t.WMBSBase import WMBSBase
from WMCore.Database.DBFactory import DBFactory

from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Workflow import Workflow

class SubscriptionTest(WMBSBase):
    """
    __SubscriptionTest__

     Performance testcase for Subscription DAO class

     This class is abstract, proceed to the DB specific testcase
     to run the test


    """
    
    def setUp(self, sqlURI='', logarg=''):
        #Call common setUp method from WMBSBase
                
        self.logger = logging.getLogger(logarg + 'SubscriptionPerformanceTest')
        
        dbf = DBFactory(self.logger, sqlURI)
        
        WMBSBase.setUp(self,dbf=dbf)

    def tearDown(self):
        #Call superclass tearDown method
        WMBSBase.tearDown(self)

    def testAcquireFiles(self):         
        print "testAcquireFiles"

        time = self.perfTest(dao=self.dao, action='Subscriptions.AcquireFiles', subscription=self.testSubscription.id,file=self.testFile["id"])
        assert time <= self.threshold, 'AcquireFiles DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testCompleteFiles(self):         
        print "testCompleteFiles"
   
        time = self.perfTest(dao=self.dao, action='Subscriptions.CompleteFiles', subscription=self.testSubscription.id,file=self.testFile["id"])
        assert time <= self.threshold, 'CompleteFiles DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testFailFiles(self):         
        print "testFailFiles"

        time = self.perfTest(dao=self.dao, action='Subscriptions.FailFiles', subscription=self.testSubscription.id,file=self.testFile["id"])
        assert time <= self.threshold, 'FailFiles DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testDeleteAcquiredFiles(self):         
        print "testDeleteAcquiredFiles"

        time = self.perfTest(dao=self.dao, action='Subscriptions.DeleteAcquiredFiles', subscription=self.testSubscription.id,file=self.testFile["id"])
        assert time <= self.threshold, 'DeleteAcquiredFiles DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testGetAcquiredFiles(self):         
        print "testGetAcquiredFiles"
        
        time = self.perfTest(dao=self.dao, action='Subscriptions.GetAcquiredFiles', subscription=self.testSubscription.id)
        assert time <= self.threshold, 'GetAcquiredFiles DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testGetAvailableFiles(self):         
        print "testGetAvailableFiles"

        time = self.perfTest(dao=self.dao, action='Subscriptions.GetAvailableFiles', subscription=self.testSubscription.id)
        assert time <= self.threshold, 'GetAvailableFiles DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testGetCompletedFiles(self):         
        print "testGetCompletedFiles"

        time = self.perfTest(dao=self.dao, action='Subscriptions.GetCompletedFiles', subscription=self.testSubscription.id)
        assert time <= self.threshold, 'GetCompletedFiles DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testGetFailedFiles(self):         
        print "testGetFailedFiles"

        time = self.perfTest(dao=self.dao, action='Subscriptions.GetFailedFiles', subscription=self.testSubscription.id)
        assert time <= self.threshold, 'GetFailedFiles DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testForFileset(self):         
        print "testForFileset"

        time = self.perfTest(dao=self.dao, action='Subscriptions.ForFileset', fileset=self.testFileset.id)
        assert time <= self.threshold, 'ForFileset DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testNew(self):         
        print "testNew"

        time = self.perfTest(dao=self.dao, action='Subscriptions.New', fileset=self.testFileset.id, workflow=self.testWorkflow.id, type='Merge')
        assert time <= self.threshold, 'New DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testLoad(self):         
        print "testLoad"

        time = self.perfTest(dao=self.dao, action='Subscriptions.Load', workflow=self.testWorkflow.id, type='Processing', fileset=self.testFileset.id)
        assert time <= self.threshold, 'Load DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testJobs(self):         
        print "testJobs"

        time = self.perfTest(dao=self.dao, action='Subscriptions.Jobs', subscription=self.testSubscription.id)
        assert time <= self.threshold, 'Jobs DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testExists(self):         
        print "testExists"
        
        time = self.perfTest(dao=self.dao, action='Subscriptions.Exists', workflow=self.testWorkflow.id, fileset=self.testFileset.id,
type='Processing')
        assert time <= self.threshold, 'Exists DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'


