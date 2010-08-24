#!/usr/bin/env python

import logging
from WMCore_t.WMBS_t.Performance_t.Base_t import BaseTest
from WMCore.Database.DBFactory import DBFactory

class JobGroupTest(BaseTest):
    """
    __JobGroupTest__

     Performance testcase for WMBS JobGroup class

     This class is abstract, proceed to the DB specific testcase
     to run the test


    """
    
    def setUp(self, sqlURI='', logarg=''):
        #Call common setUp method from BaseTest
                
        self.logger = logging.getLogger(logarg + 'JobGroupPerformanceTest')
        
        dbf = DBFactory(self.logger, sqlURI)
        
        BaseTest.setUp(self,dbf=dbf)

    def tearDown(self):
        #Call superclass tearDown method
        BaseTest.tearDown(self)

    def testNew(self):         
        print "testNew"
        
        #Surprisingly it worked with the same Subscription
        #TODO - Validate if its working properly
        time = self.perfTest(dao=self.dao, action='JobGroup.New', subscription=self.testSubscription.id)
        assert time <= self.threshold, 'New DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testLoad(self):   
        # Still no complete JobGroup.Load class
        print "testLoad"
        
        time = self.perfTest(dao=self.dao, action='JobGroup.Load')
        assert time <= self.threshold, 'Load DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testStatus(self):         
        print "testStatus"
        
        time = self.perfTest(dao=self.dao, action='JobGroup.Status', group=self.testJobGroup.id)
        assert time <= self.threshold, 'Status DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'


