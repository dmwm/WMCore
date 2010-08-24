#!/usr/bin/env python

import unittest, time, random

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.Base_t import Base_t
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.Subscription import Subscription

class Subscription_t(Base_t,TestCase):
    """
    __Subscription_t__

     DB Performance testcase for WMBS Subscription class


    """

    def setUp(self):
        #Call common setUp method from Base_t
        Base_t.setUp(self)
        self.threshold = 1

    def tearDown(self):
        #Call superclass tearDown method
        Base_t.tearDown(self)

    def testAcquireFiles(self):         
        print "testAcquireFiles"
        
        time = self.perfTest(dao=self.mysqldao, action='Subscriptions.AcquireFiles', execinput=['subscription=self.testmysqlSubscription.id','file=self.testmysqlFile'])
        assert time <= self.threshold, 'AcquireFiles DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testCompleteFiles(self):         
        print "testCompleteFiles"

        time = self.perfTest(dao=self.mysqldao, action='Subscriptions.CompleteFiles', execinput=['subscription=self.testmysqlSubscription','file=self.mysqlFile'])
        assert time <= self.threshold, 'CompleteFiles DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testFailFiles(self):         
        print "testFailFiles"

        time = self.perfTest(dao=self.mysqldao, action='Subscriptions.FailFiles', execinput=['subscription=self.testmysqlSubscription','file=self.mysqlFile'])
        assert time <= self.threshold, 'FailFiles DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testDeleteAcquiredFiles(self):         
        print "testDeleteAcquiredFiles"

        time = self.perfTest(dao=self.mysqldao, action='Subscriptions.DeleteAcquiredFiles', execinput=['subscription=self.testmysqlSubscription','file=self.mysqlFile'])
        assert time <= self.threshold, 'DeleteAcquiredFiles DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testGetAcquiredFiles(self):         
        print "testGetAcquiredFiles"
        
        time = self.perfTest(dao=self.mysqldao, action='Subscriptions.GetAcquiredFiles', execinput=['subscription=self.testmysqlSubscription'])
        assert time <= self.threshold, 'GetAcquiredFiles DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testGetAvailableFiles(self):         
        print "testGetAvailableFiles"

        time = self.perfTest(dao=self.mysqldao, action='Subscriptions.GetAvailableFiles', execinput=['subscription=self.testmysqlSubscription.id'])
        assert time <= self.threshold, 'GetAvailableFiles DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testGetCompletedFiles(self):         
        print "testGetCompletedFiles"

        time = self.perfTest(dao=self.mysqldao, action='Subscriptions.GetCompletedFiles', execinput=['subscription=self.testmysqlSubscription.id'])
        assert time <= self.threshold, 'GetCompletedFiles DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testGetFailedFiles(self):         
        print "testGetFailedFiles"

        time = self.perfTest(dao=self.mysqldao, action='Subscriptions.GetFailedFiles', execinput=['subscription=self.testmysqlSubscription.id'])
        assert time <= self.threshold, 'GetFailedFiles DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testForFileset(self):         
        print "testForFileset"

        time = self.perfTest(dao=self.mysqldao, action='Subscriptions.ForFileset', execinput=['subscription=self.testmysqlSubscription','fileset=self.mysqlFileset'])
        assert time <= self.threshold, 'ForFileset DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testNew(self):         
        print "testNew"

        time = self.perfTest(dao=self.mysqldao, action='Subscriptions.New', execinput=['fileset=self.mysqlFileset', 'workflow=self.testmysqlWorkflow', 'spec=TestWorkflowSpec','owner=Performance Testcase'])
        assert time <= self.threshold, 'New DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testLoad(self):         
        print "testLoad"

        time = self.perfTest(dao=self.mysqldao, action='Subscriptions.Load', execinput=['workflow=self.testmysqlWorkflow', 'type=Merge', 'fileset=self.testmysqlFileset'])
        assert time <= self.threshold, 'Load DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testJobs(self):         
        print "testJobs"

        time = self.perfTest(dao=self.mysqldao, action='Subscriptions.Jobs', execinput=['subscription=self.testmysqlSubscription.id'])
        assert time <= self.threshold, 'Jobs DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testExists(self):         
        print "testExists"
        
        time = self.perfTest(dao=self.mysqldao, action='Subscriptions.Exists', execinput=['workflow=self.testmysqlWorkflow','fileset=self.testmysqlFileset',
'type=Merge'])
        assert time <= self.threshold, 'Exists DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

if __name__ == "__main__":
    unittest.main()

