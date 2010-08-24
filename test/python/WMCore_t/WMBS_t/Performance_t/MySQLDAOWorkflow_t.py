#!/usr/bin/env python

import unittest

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.Base_t import Base_t

class Workflow_t(Base_t,TestCase):
    """
    __MySQLDAOWorkflow_t__

     MySQL DAO Performance testcase for WMBS Workflow class


    """

    def setUp(self):
        #Call common setUp method from Base_t
        Base_t.setUp(self)
        self.threshold = 1

    def tearDown(self):
        #Call superclass tearDown method
        Base_t.tearDown(self)

    def testExists(self):         
        print "testExists"

        time = self.perfTest(dao=self.mysqldao, action='Workflow.Exists', execinput=['spec="TestDAO"', 'owner="PerformanceTestCaseDAO"', 'name="Test_mysqlWorkflow"'])
        assert time <= self.threshold, 'Exists DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testNew(self):         
        print "testNew"

        time = self.perfTest(dao=self.mysqldao, action='Workflow.New', execinput=['workflow=5','fileset=1',
'type=1'])
        assert time <= self.threshold, 'New DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testDelete(self):         
        print "testDelete"

        time = self.perfTest(dao=self.mysqldao, action='Workflow.Delete', execinput=['id=self.testmysqlWorkflow.id'])
        assert time <= self.threshold, 'Delete DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testLoadFromID(self):         
        print "testLoadFromID"

        time = self.perfTest(dao=self.mysqldao, action='Workflow.LoadFromID', execinput=['workflow=self.testmysqlWorkflow.id'])
        assert time <= self.threshold, 'LoadFromID DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testLoadFromName(self):         
        print "testLoadFromName"

        time = self.perfTest(dao=self.mysqldao, action='Workflow.LoadFromName', execinput=['workflow=self.testmysqlWorkflow.name'])
        assert time <= self.threshold, 'LoadFromName DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testLoadSpecOwner(self):         
        print "testLoadSpecOwner"

        time = self.perfTest(dao=self.mysqldao, action='Workflow.LoadSpecOwner', execinput=['spec="Test", owner="PerformanceTestCase"'])
        assert time <= self.threshold, 'LoadSpecOwner DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

if __name__ == "__main__":
    unittest.main()

