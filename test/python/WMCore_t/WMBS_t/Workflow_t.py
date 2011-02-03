#!/usr/bin/env python
"""
_Workflow_t_

Unit tests for the WMBS Workflow class.
"""

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
        return
                       
    def tearDown(self):
        """
        _tearDown_

        Drop all the WMBS tables.
        """
        self.testInit.clearDatabase()
        return

    def testCreateDeleteExists(self):
        """
        _testCreateDeleteExists_

        Test the create(), delete() and exists() methods of the workflow class
        by creating and deleting a workflow.  The exists() method will be
        called before and after creation and after deletion.
        """
        testWorkflow = Workflow(spec = "spec.xml", owner = "Simon",
                                name = "wf001", task='Test')

        self.assertEqual(testWorkflow.exists(), False,
                         "ERROR: Workflow exists before it was created")

        testWorkflow.create()

        self.assertTrue(testWorkflow.exists() > 0,
                        "ERROR: Workflow does not exist after it has been created")

        testWorkflow.create()
        testWorkflow.delete()

        self.assertEqual(testWorkflow.exists(), False,
                         "ERROR: Workflow exists after it has been deleted")
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

        self.assertEqual(testWorkflow.exists(), False,
                         "ERROR: Workflow exists before it was created")

        testWorkflow.create()

        self.assertTrue(testWorkflow.exists() > 0,
                        "ERROR: Workflow does not exist after it has been created")

        myThread.transaction.rollback()

        self.assertEqual(testWorkflow.exists(), False,
                         "ERROR: Workflow exists after the transaction was rolled back.")
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

        self.assertEqual(testWorkflow.exists(), False,
                         "ERROR: Workflow exists before it was created")

        testWorkflow.create()

        self.assertTrue(testWorkflow.exists() > 0,
                        "ERROR: Workflow does not exist after it has been created")

        myThread = threading.currentThread()
        myThread.transaction.begin()

        testWorkflow.delete()

        self.assertEqual(testWorkflow.exists(), False,
                         "ERROR: Workflow exists after it has been deleted")

        myThread.transaction.rollback()

        self.assertTrue(testWorkflow.exists() > 0,
                        "ERROR: Workflow does not exist transaction was rolled back")
        
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

        self.assertTrue((testWorkflowA.id == testWorkflowB.id) and
                        (testWorkflowA.name == testWorkflowB.name) and
                        (testWorkflowA.spec == testWorkflowB.spec) and
                        (testWorkflowA.task == testWorkflowB.task) and
                        (testWorkflowA.owner == testWorkflowB.owner),
                        "ERROR: Workflow.LoadFromName Failed")
        
        testWorkflowC = Workflow(id = testWorkflowA.id)
        testWorkflowC.load()

        self.assertTrue((testWorkflowA.id == testWorkflowC.id) and
                        (testWorkflowA.name == testWorkflowC.name) and
                        (testWorkflowA.spec == testWorkflowC.spec) and
                        (testWorkflowA.task == testWorkflowC.task) and
                        (testWorkflowA.owner == testWorkflowC.owner),
                        "ERROR: Workflow.LoadFromID Failed")
        
        testWorkflowD = Workflow(spec = "spec.xml", owner = "Simon", task='Test')
        testWorkflowD.load()

        self.assertTrue((testWorkflowA.id == testWorkflowD.id) and
                        (testWorkflowA.name == testWorkflowD.name) and
                        (testWorkflowA.spec == testWorkflowD.spec) and
                        (testWorkflowA.task == testWorkflowD.task) and
                        (testWorkflowA.owner == testWorkflowD.owner),
                        "ERROR: Workflow.LoadFromSpecOwner Failed")

        testWorkflowA.delete()
        return

    def testOutput(self):
        """
        _testOutput_

        Creat a workflow and add some outputs to it.  Verify that these are
        stored to and loaded from the database correctly.
        """
        testFilesetA = Fileset(name = "testFilesetA")
        testMergedFilesetA = Fileset(name = "testMergedFilesetA")
        testFilesetB = Fileset(name = "testFilesetB")
        testMergedFilesetB = Fileset(name = "testMergedFilesetB")
        testFilesetC = Fileset(name = "testFilesetC")
        testMergedFilesetC = Fileset(name = "testMergedFilesetC")
        testFilesetA.create()
        testFilesetB.create()
        testFilesetC.create()
        testMergedFilesetA.create()
        testMergedFilesetB.create()
        testMergedFilesetC.create()
        
        testWorkflowA = Workflow(spec = "spec.xml", owner = "Simon",
                                 name = "wf001", task='Test')
        testWorkflowA.create()

        testWorkflowB = Workflow(name = "wf001", task='Test')
        testWorkflowB.load()

        self.assertEqual(len(testWorkflowB.outputMap.keys()), 0,
                         "ERROR: Output map exists before output is assigned")

        testWorkflowA.addOutput("outModOne", testFilesetA, testMergedFilesetA)
        testWorkflowA.addOutput("outModOne", testFilesetC, testMergedFilesetC)
        testWorkflowA.addOutput("outModTwo", testFilesetB, testMergedFilesetB)

        testWorkflowC = Workflow(name = "wf001", task='Test')
        testWorkflowC.load()

        self.assertEqual(len(testWorkflowC.outputMap.keys()), 2,
                         "ERROR: Incorrect number of outputs in output map")
        self.assertTrue("outModOne" in testWorkflowC.outputMap.keys(),
                        "ERROR: Output modules missing from workflow output map")
        self.assertTrue("outModTwo" in testWorkflowC.outputMap.keys(),
                        "ERROR: Output modules missing from workflow output map")

        for outputMap in testWorkflowC.outputMap["outModOne"]:
            if outputMap["output_fileset"].id == testFilesetA.id:
                self.assertEqual(outputMap["merged_output_fileset"].id,
                                 testMergedFilesetA.id,
                                 "Error: Output map incorrectly maps filesets.")
            else:
                self.assertEqual(outputMap["merged_output_fileset"].id,
                                 testMergedFilesetC.id,
                                 "Error: Output map incorrectly maps filesets.")
                self.assertEqual(outputMap["output_fileset"].id,
                                 testFilesetC.id,
                                 "Error: Output map incorrectly maps filesets.")

        self.assertEqual(testWorkflowC.outputMap["outModTwo"][0]["merged_output_fileset"].id,
                         testMergedFilesetB.id,
                         "Error: Output map incorrectly maps filesets.")
        self.assertEqual(testWorkflowC.outputMap["outModTwo"][0]["output_fileset"].id,
                         testFilesetB.id,
                         "Error: Output map incorrectly maps filesets.")
        return

    def testLoadFromTask(self):
        """
        _testLoadFromTask_

        Verify that Workflow.LoadFromTask DAO correct loads the workflow by
        task.
        """
        testWorkflow1 = Workflow(spec = "spec1.xml", owner = "Hassen",
                                 name = "wf001", task = "sometask")
        testWorkflow1.create()

        myThread = threading.currentThread()
        daoFactory = DAOFactory(package="WMCore.WMBS", logger = myThread.logger,
                                dbinterface = myThread.dbi)
        loadFromTaskDAO = daoFactory(classname = "Workflow.LoadFromTask")

        listFromTask = loadFromTaskDAO.execute(task = testWorkflow1.task)

        self.assertEqual(len(listFromTask), 1, 
                          "ERROR: listFromTask should be 1.")
        self.assertEqual(listFromTask[0]["task"], "sometask",
                         "ERROR: task should be sometask.")
        return

    def testWorkflowOwner(self):
        """
        _testWorkflowOwner_

        Verify that the user is being added and handled correctly
        """

        dn = "/C=IT/O=INFN/OU=Personal Certificate/L=Perugia/CN=Mattia " + \
             "Cinquilli/CN=proxy"
        testWorkflow1 = Workflow(spec = "spec1.xml", owner = dn,
                                 name = "wf001", task = "MultiUser-support")
        testWorkflow1.create()

        myThread = threading.currentThread()
        daoFactory = DAOFactory(package="WMCore.WMBS", logger = myThread.logger,
                                dbinterface = myThread.dbi)
        loadFromOwnerDAO = daoFactory(classname = "Workflow.LoadFromSpecOwner")

        listFromOwner1 = loadFromOwnerDAO.execute(task  = testWorkflow1.task,
                                                  owner = testWorkflow1.owner,
                                                  spec  = testWorkflow1.spec )

        testWorkflow2 = Workflow(spec = "spec2.xml", owner = dn,
                                 name = "wf002", task = "MultiUser-support")
        testWorkflow2.create()

        listFromOwner2 = loadFromOwnerDAO.execute(task  = testWorkflow2.task,
                                                  owner = testWorkflow2.owner,
                                                  spec  = testWorkflow2.spec )

        testWorkflow3 = Workflow(spec = "spec3.xml", owner = "Ciccio",
                                 name = "wf003", task = "MultiUser-support")
        testWorkflow3.create()

        listFromOwner3 = loadFromOwnerDAO.execute(task  = testWorkflow3.task,
                                                  owner = testWorkflow3.owner,
                                                  spec  = testWorkflow3.spec )


        self.assertEqual(testWorkflow1.owner, dn)
        self.assertEqual(listFromOwner1["owner"], dn)
        self.assertEqual(listFromOwner2["owner"], dn)
        self.assertEqual(listFromOwner1["owner"], listFromOwner2["owner"])
        self.assertNotEqual(listFromOwner1["owner"], listFromOwner3["owner"])
        self.assertNotEqual(listFromOwner2["owner"], listFromOwner3["owner"])
        return

if __name__ == "__main__":
    unittest.main()
