#!/usr/bin/env python
"""
_Workflow_t_

Unit tests for the WMBS Workflow class.
"""

__revision__ = "$Id: Workflow_t.py,v 1.9 2009/03/24 16:29:39 sfoulkes Exp $"
__version__ = "$Revision: 1.9 $"

import unittest
import os
import threading

from WMCore.WMBS.Workflow import Workflow
from WMCore.WMFactory import WMFactory
from WMQuality.TestInit import TestInit

class WorkflowTest(unittest.TestCase):
    _setup = False
    _teardown = False

    def runTest(self):
        """
        _runTest_

        Run all the unit tests.
        """
        unittest.main()
    
    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Try to create all of the
        WMBS tables.
        """
        if not self._setup:
            self.testInit = TestInit(__file__, os.getenv("DIALECT"))
            self.testInit.setLogging()
            self.testInit.setDatabaseConnection()
            self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                    useDefault = False)
            self._setup = True

        return
                       
    def tearDown(self):
        """
        _tearDown_

        Drop all the WMBS tables.
        """
        myThread = threading.currentThread()

        factory = WMFactory("WMBS", "WMCore.WMBS")
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        myThread.transaction.begin()
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete WMBS tear down.")
        myThread.transaction.commit()

    def testCreateDeleteExists(self):
        """
        _testCreateDeleteExists_

        Test the create(), delete() and exists() methods of the workflow class
        by creating and deleting a workflow.  The exists() method will be
        called before and after creation and after deletion.
        """
        testWorkflow = Workflow(spec = "spec.xml", owner = "Simon",
                                name = "wf001")

        assert testWorkflow.exists() == False, \
               "ERROR: Workflow exists before it was created"

        testWorkflow.create()

        assert testWorkflow.exists() > 0, \
               "ERROR: Workflow does not exist after it has been created"

        testWorkflow.create()
        testWorkflow.delete()

        assert testWorkflow.exists() == False, \
               "ERROR: Workflow exists after it has been deleted"        
        return

    def testCreateTransaction(self):
        """
        _testCreateTransaction_

        Create a workflow and commit it to the database and then roll the
        transaction back.  Use the workflow's exists() method to verify that the
        workflow does not exist before create() is called, exists after create()
        is called and does not exist after the transaction is rolled back.
        """
        myThread = threading.currentThread()
        myThread.transaction.begin()

        testWorkflow = Workflow(spec = "spec.xml", owner = "Simon",
                                name = "wf001")

        assert testWorkflow.exists() == False, \
               "ERROR: Workflow exists before it was created"

        testWorkflow.create()

        assert testWorkflow.exists() > 0, \
               "ERROR: Workflow does not exist after it has been created"

        myThread.transaction.rollback()

        assert testWorkflow.exists() == False, \
               "ERROR: Workflow exists after the transaction was rolled back."
        return

    def testDeleteTransaction(self):
        """
        _testDeleteTransaction_

        Create a workflow and commit it to the database.  Begin a transaction
        and delete the workflow, then rollback the transaction.  Use the
        workflow's exists() method to verify that the workflow doesn't exist
        in the database before create() is called, it does exist after create()
        is called, it doesn't exist after delete() is called and it does exist
        after the transaction is rolled back.
        """
        testWorkflow = Workflow(spec = "spec.xml", owner = "Simon",
                                name = "wf001")

        assert testWorkflow.exists() == False, \
               "ERROR: Workflow exists before it was created"

        testWorkflow.create()

        assert testWorkflow.exists() > 0, \
               "ERROR: Workflow does not exist after it has been created"

        myThread = threading.currentThread()
        myThread.transaction.begin()

        testWorkflow.delete()

        assert testWorkflow.exists() == False, \
               "ERROR: Workflow exists after it has been deleted"

        myThread.transaction.rollback()

        assert testWorkflow.exists() > 0, \
               "ERROR: Workflow does not exist transaction was rolled back"
        
        return

    def testLoad(self):
        """
        _testLoad_

        Create a workflow and then try to load it from the database using the
        following load methods:
          Workflow.LoadFromName
          Workflow.LoadFromID
          Workflow.LoadFromSpecOwner
        """
        testWorkflowA = Workflow(spec = "spec.xml", owner = "Simon",
                                 name = "wf001")
        testWorkflowA.create()

        testWorkflowB = Workflow(name = "wf001")
        testWorkflowB.load()

        assert type(testWorkflowB.id) == int, \
               "ERROR: Workflow id is not an int."

        assert (testWorkflowA.id == testWorkflowB.id) and \
               (testWorkflowA.name == testWorkflowB.name) and \
               (testWorkflowA.spec == testWorkflowB.spec) and \
               (testWorkflowA.owner == testWorkflowB.owner), \
               "ERROR: Workflow.LoadFromName Failed"
        
        testWorkflowC = Workflow(id = testWorkflowA.id)
        testWorkflowC.load()

        assert type(testWorkflowC.id) == int, \
               "ERROR: Workflow id is not an int."
        
        assert (testWorkflowA.id == testWorkflowC.id) and \
               (testWorkflowA.name == testWorkflowC.name) and \
               (testWorkflowA.spec == testWorkflowC.spec) and \
               (testWorkflowA.owner == testWorkflowC.owner), \
               "ERROR: Workflow.LoadFromID Failed"
        
        testWorkflowD = Workflow(spec = "spec.xml", owner = "Simon")
        testWorkflowD.load()

        assert type(testWorkflowD.id) == int, \
               "ERROR: Workflow id is not an int."
        
        assert (testWorkflowA.id == testWorkflowD.id) and \
               (testWorkflowA.name == testWorkflowD.name) and \
               (testWorkflowA.spec == testWorkflowD.spec) and \
               (testWorkflowA.owner == testWorkflowD.owner), \
               "ERROR: Workflow.LoadFromSpecOwner Failed"

        testWorkflowA.delete()
        return
                                            
if __name__ == "__main__":
    unittest.main()
