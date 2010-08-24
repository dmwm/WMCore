#!/usr/bin/env python

import logging
from WMCore_t.WMBS_t.Performance_t.Base_t import Base_t
from WMCore.Database.DBFactory import DBFactory

class JobGroup_t(Base_t):
    """
    __JobGroup_t__

     Performance testcase for WMBS JobGroup class

     This class is abstract, proceed to the DB specific testcase
     to run the test


    """
    
    def setUp(self, sqlURI='', logarg=''):
        #Call common setUp method from Base_t
                
        self.logger = logging.getLogger(logarg + 'JobGroupPerformanceTest')
        
        dbf = DBFactory(self.logger, sqlURI)
        
        Base_t.setUp(self,dbf=dbf)

    def tearDown(self):
        #Call superclass tearDown method
        Base_t.tearDown(self)

    def testNew(self):         
        print "testNew"
        
        time = self.perfTest(dao=self.dao, action='JobGroup.New', execinput=['subscription=self.testSubscription.id'])
        assert time <= self.threshold, 'New DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testLoad(self):         
        print "testLoad"
        
        time = self.perfTest(dao=self.dao, action='JobGroup.Load', execinput=[''])
        assert time <= self.threshold, 'Load DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testStatus(self):         
        print "testStatus"
        
        time = self.perfTest(dao=self.dao, action='JobGroup.Status', execinput=['group=self.testJobGroup.id'])
        assert time <= self.threshold, 'Status DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'


