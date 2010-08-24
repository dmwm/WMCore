#!/usr/bin/env python

import logging
from WMCore_t.WMBS_t.Performance_t.Base_t import Base_t
from WMCore.Database.DBFactory import DBFactory

class Subscription_t(Base_t):
    """
    __Subscription_t__

     Performance testcase for Subscription DAO class

     This class is abstract, proceed to the DB specific testcase
     to run the test


    """
    
    def setUp(self, sqlURI='', logarg=''):
        #Call common setUp method from Base_t
                
        self.logger = logging.getLogger(logarg + 'SubscriptionPerformanceTest')
        
        dbf = DBFactory(self.logger, sqlURI)
        
        Base_t.setUp(self,dbf=dbf)

    def tearDown(self):
        #Call superclass tearDown method
        Base_t.tearDown(self)

    def testAcquireFiles(self):         
        print "testAcquireFiles"

        time = self.perfTest(dao=self.dao, action='Subscriptions.AcquireFiles', execinput=['subscription=self.testSubscription.id','file=self.testFile'])
        assert time <= self.threshold, 'AcquireFiles DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testCompleteFiles(self):         
        print "testCompleteFiles"
   
        time = self.perfTest(dao=self.dao, action='Subscriptions.CompleteFiles', execinput=['subscription=self.testSubscription','file=self.testFile'])
        assert time <= self.threshold, 'CompleteFiles DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testFailFiles(self):         
        print "testFailFiles"

        time = self.perfTest(dao=self.dao, action='Subscriptions.FailFiles', execinput=['subscription=self.testSubscription','file=self.testFile'])
        assert time <= self.threshold, 'FailFiles DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testDeleteAcquiredFiles(self):         
        print "testDeleteAcquiredFiles"

        time = self.perfTest(dao=self.dao, action='Subscriptions.DeleteAcquiredFiles', execinput=['subscription=self.testSubscription','file=self.testFile'])
        assert time <= self.threshold, 'DeleteAcquiredFiles DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testGetAcquiredFiles(self):         
        print "testGetAcquiredFiles"
        
        time = self.perfTest(dao=self.dao, action='Subscriptions.GetAcquiredFiles', execinput=['subscription=self.testSubscription'])
        assert time <= self.threshold, 'GetAcquiredFiles DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testGetAvailableFiles(self):         
        print "testGetAvailableFiles"

        time = self.perfTest(dao=self.dao, action='Subscriptions.GetAvailableFiles', execinput=['subscription=self.testSubscription.id'])
        assert time <= self.threshold, 'GetAvailableFiles DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testGetCompletedFiles(self):         
        print "testGetCompletedFiles"

        time = self.perfTest(dao=self.dao, action='Subscriptions.GetCompletedFiles', execinput=['subscription=self.testSubscription.id'])
        assert time <= self.threshold, 'GetCompletedFiles DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testGetFailedFiles(self):         
        print "testGetFailedFiles"

        time = self.perfTest(dao=self.dao, action='Subscriptions.GetFailedFiles', execinput=['subscription=self.testSubscription.id'])
        assert time <= self.threshold, 'GetFailedFiles DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testForFileset(self):         
        print "testForFileset"

        time = self.perfTest(dao=self.dao, action='Subscriptions.ForFileset', execinput=['subscription=self.testSubscription','fileset=self.testFileset'])
        assert time <= self.threshold, 'ForFileset DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testNew(self):         
        print "testNew"

        time = self.perfTest(dao=self.dao, action='Subscriptions.New', execinput=['fileset=self.testFileset', 'workflow=self.testWorkflow', 'spec=TestWorkflowSpec','owner=Performance Testcase'])
        assert time <= self.threshold, 'New DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testLoad(self):         
        print "testLoad"

        time = self.perfTest(dao=self.dao, action='Subscriptions.Load', execinput=['workflow=self.testWorkflow', 'type=Merge', 'fileset=self.testFileset'])
        assert time <= self.threshold, 'Load DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testJobs(self):         
        print "testJobs"

        time = self.perfTest(dao=self.dao, action='Subscriptions.Jobs', execinput=['subscription=self.testSubscription.id'])
        assert time <= self.threshold, 'Jobs DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testExists(self):         
        print "testExists"
        
        time = self.perfTest(dao=self.dao, action='Subscriptions.Exists', execinput=['workflow=self.testWorkflow','fileset=self.testFileset',
'type=Merge'])
        assert time <= self.threshold, 'Exists DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'


