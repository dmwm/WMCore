#!/usr/bin/env python

import unittest
from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.Base_t import Base_t

class Job_t(Base_t,TestCase):
    """
    __Job_t__

     DB Performance testcase for WMBS Job class


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
        
        time = self.perfTest(dao=self.mysqldao, action='Jobs.New', execinput=['jobgroup=self.testmysqlJobGroup.id','name=testmysqlJobGroup.name'])
        assert time <= self.threshold, 'New DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testActive(self):         
        print "testActive"
        
        time = self.perfTest(dao=self.mysqldao, action='Jobs.Active', execinput=['job=self.testmysqlJob.id'])
        assert time <= self.threshold, 'Active DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testComplete(self):         
        print "testComplete"
        
        time = self.perfTest(dao=self.mysqldao, action='Jobs.Complete', execinput=['job=self.testmysqlJob.id'])
        assert time <= self.threshold, 'Complete DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testFailed(self):         
        print "testFailed"
        
        time = self.perfTest(dao=self.mysqldao, action='Jobs.Failed', execinput=['job=self.testmysqlJob.id'])
        assert time <= self.threshold, 'Failed DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testLoad(self):         
        print "testLoad"
        
        time = self.perfTest(dao=self.mysqldao, action='Jobs.Load', execinput=['id=self.testmysqlJob.id'])
        assert time <= self.threshold, 'Load DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testClearStatus(self):         
        print "testClearStatus"
        
        time = self.perfTest(dao=self.mysqldao, action='Jobs.ClearStatus', execinput=['job=self.testmysqlJob.id'])
        assert time <= self.threshold, 'ClearStatus DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testUpdateName(self):         
        print "testUpdateName"
        
        time = self.perfTest(dao=self.mysqldao, action='Jobs.UpdateName', execinput=['id=self.testmysqlJob.id','name=self.testmysqlJob.name'])
        assert time <= self.threshold, 'UpdateName DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testAddFiles(self):         
        print "testAddFiles"
        
        time = self.perfTest(dao=self.mysqldao, action='Jobs.AddFiles', execinput=['id=self.testmysqlJob.id','file=testmysqlFile.id'])
        assert time <= self.threshold, 'AddFiles DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

if __name__ == "__main__":
    unittest.main()
