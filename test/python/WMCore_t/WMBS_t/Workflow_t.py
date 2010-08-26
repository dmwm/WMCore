#!/usr/bin/env python
"""
_Workflow_t_

Unit tests for the WMBS Workflow class.
"""

__revision__ = "$Id: Workflow_t.py,v 1.16 2010/06/01 17:28:43 riahi Exp $"
__version__ = "$Revision: 1.16 $"

import unittest
import os
import threading

from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.Fileset import Fileset
from WMCore.DAOFactory import DAOFactory

from WMCore.WMFactory import WMFactory
from WMQuality.TestInit import TestInit

class WorkflowTest(unittest.TestCase):

    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Try to create all of the
        WMBS tables.
        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)

                       
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
                                name = "wf001", task='Test')

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
                                name = "wf001", task='Test')

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
                                name = "wf001", task='Test')

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
                                 name = "wf001", task='Test')
        testWorkflowA.create()

        testWorkflowB = Workflow(name = "wf001", task='Test')
        testWorkflowB.load()

        assert type(testWorkflowB.id) == int, \
               "ERROR: Workflow id is not an int."

        assert (testWorkflowA.id == testWorkflowB.id) and \
               (testWorkflowA.name == testWorkflowB.name) and \
               (testWorkflowA.spec == testWorkflowB.spec) and \
               (testWorkflowA.task == testWorkflowB.task) and \
               (testWorkflowA.owner == testWorkflowB.owner), \
               "ERROR: Workflow.LoadFromName Failed"
        
        testWorkflowC = Workflow(id = testWorkflowA.id)
        testWorkflowC.load()

        assert type(testWorkflowC.id) == int, \
               "ERROR: Workflow id is not an int."
        
        assert (testWorkflowA.id == testWorkflowC.id) and \
               (testWorkflowA.name == testWorkflowC.name) and \
               (testWorkflowA.spec == testWorkflowC.spec) and \
               (testWorkflowA.task == testWorkflowC.task) and \
               (testWorkflowA.owner == testWorkflowC.owner), \
               "ERROR: Workflow.LoadFromID Failed"
        
        testWorkflowD = Workflow(spec = "spec.xml", owner = "Simon", task='Test')
        testWorkflowD.load()

        assert type(testWorkflowD.id) == int, \
               "ERROR: Workflow id is not an int."
        
        assert (testWorkflowA.id == testWorkflowD.id) and \
               (testWorkflowA.name == testWorkflowD.name) and \
               (testWorkflowA.spec == testWorkflowD.spec) and \
               (testWorkflowA.task == testWorkflowD.task) and \
               (testWorkflowA.owner == testWorkflowD.owner), \
               "ERROR: Workflow.LoadFromSpecOwner Failed"

        testWorkflowA.delete()
        return

    def testOutput(self):
        """
        _testOutput_

        Creat a workflow and add some outputs to it.  Verify that these are
        stored to and loaded from the database correctly.
        """
        testFilesetA = Fileset(name = "testFilesetA")
        testFilesetB = Fileset(name = "testFilesetB")
        testFilesetA.create()
        testFilesetB.create()
        
        testWorkflowA = Workflow(spec = "spec.xml", owner = "Simon",
                                 name = "wf001", task='Test')
        testWorkflowA.create()

        testWorkflowB = Workflow(name = "wf001", task='Test')
        testWorkflowB.load()

        assert len(testWorkflowB.outputMap.keys()) == 0, \
            "ERROR: Output map exists before output is assigned"

        testWorkflowA.addOutput("outModOne", testFilesetA, "parentA")
        testWorkflowA.addOutput("outModTwo", testFilesetB)

        testWorkflowC = Workflow(name = "wf001", task='Test')
        testWorkflowC.load()

        assert len(testWorkflowC.outputMap.keys()) == 2, \
               "ERROR: Incorrect number of outputs in output map"
        assert "outModOne" in testWorkflowC.outputMap.keys(), \
               "ERROR: Output modules missing from workflow output map"
        assert "outModTwo" in testWorkflowC.outputMap.keys(), \
               "ERROR: Output modules missing from workflow output map"        

        assert testWorkflowC.outputMap["outModOne"]["output_fileset"].id == testFilesetA.id, \
               "ERROR: Output map incorrectly maps filesets."
        assert testWorkflowC.outputMap["outModOne"]["output_parent"] == "parentA", \
               "ERROR: Output map has incorrect parent."        
        assert testWorkflowC.outputMap["outModTwo"]["output_fileset"].id == testFilesetB.id, \
               "ERROR: Output map incorrectly maps filesets."
        assert testWorkflowC.outputMap["outModTwo"]["output_parent"] == None, \
               "ERROR: Output map has incorrect parent."

        return

    def testLoadFromTask(self):
        """
        _testLoadFromTask_
        Verify that Workflow.LoadFromTask DAO correct turns
        the workflow by task
        """


        testWorkflow1 = Workflow(spec = "spec1.xml", owner = "Hassen",
                                 name = "wf001", task = "sometask")
        testWorkflow1.create()

        myThread = threading.currentThread()
        daoFactory = DAOFactory(package="WMCore.WMBS", logger = myThread.logger,
                                dbinterface = myThread.dbi)
        loadFromTaskDAO = daoFactory(classname = "Workflow.LoadFromTask")

        listFromTask = loadFromTaskDAO.execute(task=testWorkflow1.task)

        assert len(listFromTask) == 1, \
               "ERROR: listFromTask should be 1."

        assert listFromTask[0]['task'] == "sometask", \
               "ERROR: task  should be sometask."

        return

if __name__ == "__main__":
    unittest.main()
