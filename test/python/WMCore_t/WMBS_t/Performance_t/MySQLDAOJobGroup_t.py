#!/usr/bin/env python

import unittest
from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.Base_t import Base_t

class Job_t(Base_t,TestCase):
    """
    __MySQLDAOJobGroup_t__

     MySQL DAO Performance testcase for WMBS Job class


    """

    def setUp(self):
        #Call common setUp method from Base_t
        Base_t.setUp(self)
        self.threshold = 1

    def tearDown(self):
        #Call superclass tearDown method
        Base_t.tearDown(self)

    def testNew(self):         
        print "testNew"
        
        time = self.perfTest(dao=self.mysqldao, action='JobGroup.New', execinput=['subscription=self.testmysqlSubscription.id'])
        assert time <= self.threshold, 'New DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testLoad(self):         
        print "testLoad"
        
        time = self.perfTest(dao=self.mysqldao, action='JobGroup.Load', execinput=[''])
        assert time <= self.threshold, 'Load DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testStatus(self):         
        print "testStatus"
        
        time = self.perfTest(dao=self.mysqldao, action='JobGroup.Status', execinput=['group=self.testmysqlJobGroup.id'])
        assert time <= self.threshold, 'Status DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'


if __name__ == "__main__":
    unittest.main()
