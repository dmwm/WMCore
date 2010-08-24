#!/usr/bin/env python

import logging
from WMCore_t.WMBS_t.Performance_t.Base_t import BaseTest
from WMCore.Database.DBFactory import DBFactory

class WorkflowTest(BaseTest):
    """
    __WorkflowTest__

     Performance testcase for WMBS Workflow class

     This class is abstract, proceed to the DB specific testcase
     to run the test


    """
    
    def setUp(self, sqlURI='', logarg=''):
        #Call common setUp method from BaseTest
                
        self.logger = logging.getLogger(logarg + 'WorkflowPerformanceTest')
        
        dbf = DBFactory(self.logger, sqlURI)
        
        BaseTest.setUp(self,dbf=dbf)

    def tearDown(self):
        #Call superclass tearDown method
        BaseTest.tearDown(self)

    def testExists(self):         
        print "testExists"

        time = self.perfTest(dao=self.dao, action='Workflow.Exists', spec=self.testWorkflow.spec, owner=self.testWorkflow.owner, name=self.testWorkflow.name)
        assert time <= self.threshold, 'Exists DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testNew(self):         
        print "testNew"

        #Can't use testWorkflow from BaseTest
        spec='TestSpec'
        owner='WorkflowTest'
        name='NewTestWorkflow'

        #TODO - Confirm the exec input args
        time = self.perfTest(dao=self.dao, action='Workflow.New', spec=spec, owner=owner, name=name)
        assert time <= self.threshold, 'New DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testDelete(self):         
        print "testDelete"

        time = self.perfTest(dao=self.dao, action='Workflow.Delete', id=self.testWorkflow.id)
        assert time <= self.threshold, 'Delete DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testLoadFromID(self):         
        print "testLoadFromID"

        time = self.perfTest(dao=self.dao, action='Workflow.LoadFromID', workflow=self.testWorkflow.id)
        assert time <= self.threshold, 'LoadFromID DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testLoadFromName(self):         
        print "testLoadFromName"

        time = self.perfTest(dao=self.dao, action='Workflow.LoadFromName', workflow=self.testWorkflow.name)
        assert time <= self.threshold, 'LoadFromName DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testLoadSpecOwner(self):         
        print "testLoadSpecOwner"

        time = self.perfTest(dao=self.dao, action='Workflow.LoadSpecOwner', spec=self.testWorkflow.spec, owner=self.testWorkflow.owner)
        assert time <= self.threshold, 'LoadSpecOwner DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

