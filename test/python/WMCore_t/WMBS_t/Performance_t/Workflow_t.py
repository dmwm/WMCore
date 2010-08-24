#!/usr/bin/env python

import logging
from WMCore_t.WMBS_t.Performance_t.Base_t import Base_t
from WMCore.Database.DBFactory import DBFactory

class Workflow_t(Base_t):
    """
    __Workflow_t__

     Performance testcase for WMBS Workflow class

     This class is abstract, proceed to the DB specific testcase
     to run the test


    """
    
    def setUp(self, sqlURI='', logarg=''):
        #Call common setUp method from Base_t
                
        self.logger = logging.getLogger(logarg + 'WorkflowPerformanceTest')
        
        dbf = DBFactory(self.logger, sqlURI)
        
        Base_t.setUp(self,dbf=dbf)

    def tearDown(self):
        #Call superclass tearDown method
        Base_t.tearDown(self)

    def testExists(self):         
        print "testExists"

        time = self.perfTest(dao=self.dao, action='Workflow.Exists', execinput=['spec="TestDAO"', 'owner="PerformanceTestCaseDAO"', 'name="Test_Workflow"'])
        assert time <= self.threshold, 'Exists DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testNew(self):         
        print "testNew"

        #TODO - Confirm the exec input args
        time = self.perfTest(dao=self.dao, action='Workflow.New', execinput=['workflow=5','fileset=1',
'type=1'])
        assert time <= self.threshold, 'New DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testDelete(self):         
        print "testDelete"

        time = self.perfTest(dao=self.dao, action='Workflow.Delete', execinput=['id=self.testWorkflow.id'])
        assert time <= self.threshold, 'Delete DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testLoadFromID(self):         
        print "testLoadFromID"

        time = self.perfTest(dao=self.dao, action='Workflow.LoadFromID', execinput=['workflow=self.testWorkflow.id'])
        assert time <= self.threshold, 'LoadFromID DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testLoadFromName(self):         
        print "testLoadFromName"

        time = self.perfTest(dao=self.dao, action='Workflow.LoadFromName', execinput=['workflow=self.testWorkflow.name'])
        assert time <= self.threshold, 'LoadFromName DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testLoadSpecOwner(self):         
        print "testLoadSpecOwner"

        time = self.perfTest(dao=self.dao, action='Workflow.LoadSpecOwner', execinput=['spec="Test", owner="PerformanceTestCase"'])
        assert time <= self.threshold, 'LoadSpecOwner DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

