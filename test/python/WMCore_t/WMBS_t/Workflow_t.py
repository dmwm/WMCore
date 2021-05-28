#!/usr/bin/env python
"""
_Workflow_t_

Unit tests for the WMBS Workflow class.
"""

from builtins import range

import threading
import unittest

from WMCore.DAOFactory import DAOFactory
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
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
        self.testInit.setSchema(customModules=["WMCore.WMBS"],
                                useDefault=False)
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
        testWorkflow = Workflow(spec="spec.xml", owner="Simon",
                                name="wf001", task='Test', wfType="ReReco")

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

        testWorkflow = Workflow(spec="spec.xml", owner="Simon",
                                name="wf001", task='Test')

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
        testWorkflow = Workflow(spec="spec.xml", owner="Simon",
                                name="wf001", task='Test')

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
          Workflow.LoadFromNameAndTask
          Workflow.LoadFromID
          Workflow.LoadFromSpecOwner
        """
        testWorkflowA = Workflow(spec="spec.xml", owner="Simon",
                                 name="wf001", task='Test', wfType="ReReco",
                                 priority=1000)
        testWorkflowA.create()

        testWorkflowB = Workflow(name="wf001", task='Test')
        testWorkflowB.load()

        self.assertTrue((testWorkflowA.id == testWorkflowB.id) and
                        (testWorkflowA.name == testWorkflowB.name) and
                        (testWorkflowA.spec == testWorkflowB.spec) and
                        (testWorkflowA.task == testWorkflowB.task) and
                        (testWorkflowA.wfType == testWorkflowB.wfType) and
                        (testWorkflowA.owner == testWorkflowB.owner) and
                        (testWorkflowA.priority == testWorkflowB.priority),
                        "ERROR: Workflow.LoadFromNameAndTask Failed")

        testWorkflowC = Workflow(id=testWorkflowA.id)
        testWorkflowC.load()

        self.assertTrue((testWorkflowA.id == testWorkflowC.id) and
                        (testWorkflowA.name == testWorkflowC.name) and
                        (testWorkflowA.spec == testWorkflowC.spec) and
                        (testWorkflowA.task == testWorkflowC.task) and
                        (testWorkflowA.wfType == testWorkflowC.wfType) and
                        (testWorkflowA.owner == testWorkflowC.owner) and
                        (testWorkflowA.priority == testWorkflowB.priority),
                        "ERROR: Workflow.LoadFromID Failed")

        testWorkflowD = Workflow(spec="spec.xml", owner="Simon", task='Test')
        testWorkflowD.load()

        self.assertTrue((testWorkflowA.id == testWorkflowD.id) and
                        (testWorkflowA.name == testWorkflowD.name) and
                        (testWorkflowA.spec == testWorkflowD.spec) and
                        (testWorkflowA.task == testWorkflowD.task) and
                        (testWorkflowA.wfType == testWorkflowD.wfType) and
                        (testWorkflowA.owner == testWorkflowD.owner) and
                        (testWorkflowA.priority == testWorkflowB.priority),
                        "ERROR: Workflow.LoadFromSpecOwner Failed")
        testWorkflowA.delete()
        return

    def testCreateLoadWithUserAtt(self):
        """
        _testLoad_

        Create a workflow and then try to load it from the database using the
        following load methods:
          Workflow.LoadFromNameAndTask
          Workflow.LoadFromID
          Workflow.LoadFromSpecOwner
        Test if the Workflow is created correctly.
        """
        testWorkflowA = Workflow(spec="spec.xml", owner="Simon",
                                 owner_vogroup='integration', owner_vorole='priority',
                                 name="wf001", task='Test', wfType="ReReco")
        testWorkflowA.create()

        testWorkflowB = Workflow(name="wf001", task='Test')
        testWorkflowB.load()

        self.assertTrue((testWorkflowA.id == testWorkflowB.id) and
                        (testWorkflowA.name == testWorkflowB.name) and
                        (testWorkflowA.spec == testWorkflowB.spec) and
                        (testWorkflowA.task == testWorkflowB.task) and
                        (testWorkflowA.wfType == testWorkflowB.wfType) and
                        (testWorkflowA.owner == testWorkflowB.owner),
                        "ERROR: Workflow.LoadFromNameAndTask Failed")

        testWorkflowC = Workflow(id=testWorkflowA.id)
        testWorkflowC.load()

        self.assertTrue((testWorkflowA.id == testWorkflowC.id) and
                        (testWorkflowA.name == testWorkflowC.name) and
                        (testWorkflowA.spec == testWorkflowC.spec) and
                        (testWorkflowA.task == testWorkflowC.task) and
                        (testWorkflowA.wfType == testWorkflowC.wfType) and
                        (testWorkflowA.owner == testWorkflowC.owner),
                        "ERROR: Workflow.LoadFromID Failed")

        testWorkflowD = Workflow(spec="spec.xml", owner="Simon", task='Test')
        testWorkflowD.load()

        self.assertTrue((testWorkflowA.id == testWorkflowD.id) and
                        (testWorkflowA.name == testWorkflowD.name) and
                        (testWorkflowA.spec == testWorkflowD.spec) and
                        (testWorkflowA.task == testWorkflowD.task) and
                        (testWorkflowA.wfType == testWorkflowD.wfType) and
                        (testWorkflowA.owner == testWorkflowD.owner),
                        "ERROR: Workflow.LoadFromSpecOwner Failed")

        testWorkflowE = Workflow(spec="spec_1.xml", owner="Simon",
                                 owner_vogroup='t1access', owner_vorole='t1access',
                                 name="wf002", task='Test_1', wfType="ReReco")
        testWorkflowE.create()

        self.assertTrue((testWorkflowE.id != testWorkflowA.id) and
                        (testWorkflowE.name != testWorkflowA.name) and
                        (testWorkflowE.spec != testWorkflowA.spec) and
                        (testWorkflowE.vogroup != testWorkflowA.vogroup) and
                        (testWorkflowE.vorole != testWorkflowA.vorole) and
                        (testWorkflowE.task != testWorkflowA.task) and
                        (testWorkflowE.wfType == testWorkflowA.wfType) and
                        (testWorkflowE.owner == testWorkflowA.owner),
                        "ERROR: Workflow.LoadFromSpecOwner Failed")

        testWorkflowA.delete()
        return

    def testOutput(self):
        """
        _testOutput_

        Creat a workflow and add some outputs to it.  Verify that these are
        stored to and loaded from the database correctly.
        """
        testFilesetA = Fileset(name="testFilesetA")
        testMergedFilesetA = Fileset(name="testMergedFilesetA")
        testFilesetB = Fileset(name="testFilesetB")
        testMergedFilesetB = Fileset(name="testMergedFilesetB")
        testFilesetC = Fileset(name="testFilesetC")
        testMergedFilesetC = Fileset(name="testMergedFilesetC")
        testFilesetA.create()
        testFilesetB.create()
        testFilesetC.create()
        testMergedFilesetA.create()
        testMergedFilesetB.create()
        testMergedFilesetC.create()

        testWorkflowA = Workflow(spec="spec.xml", owner="Simon",
                                 name="wf001", task='Test')
        testWorkflowA.create()

        testWorkflowB = Workflow(name="wf001", task='Test')
        testWorkflowB.load()

        self.assertEqual(len(testWorkflowB.outputMap), 0,
                         "ERROR: Output map exists before output is assigned")

        testWorkflowA.addOutput("outModOne", testFilesetA, testMergedFilesetA)
        testWorkflowA.addOutput("outModOne", testFilesetC, testMergedFilesetC)
        testWorkflowA.addOutput("outModTwo", testFilesetB, testMergedFilesetB)

        testWorkflowC = Workflow(name="wf001", task='Test')
        testWorkflowC.load()

        self.assertEqual(len(testWorkflowC.outputMap), 2,
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
        testWorkflow1 = Workflow(spec="spec1.xml", owner="Hassen",
                                 name="wf001", task="sometask")
        testWorkflow1.create()

        myThread = threading.currentThread()
        daoFactory = DAOFactory(package="WMCore.WMBS", logger=myThread.logger,
                                dbinterface=myThread.dbi)
        loadFromTaskDAO = daoFactory(classname="Workflow.LoadFromTask")

        listFromTask = loadFromTaskDAO.execute(task=testWorkflow1.task)

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

        owner = "Spiga"
        testWorkflow1 = Workflow(spec="spec1.xml", owner=owner,
                                 name="wf001", task="MultiUser-support")
        testWorkflow1.create()

        myThread = threading.currentThread()
        daoFactory = DAOFactory(package="WMCore.WMBS", logger=myThread.logger,
                                dbinterface=myThread.dbi)
        loadFromOwnerDAO = daoFactory(classname="Workflow.LoadFromSpecOwner")

        listFromOwner1 = loadFromOwnerDAO.execute(task=testWorkflow1.task,
                                                  dn=testWorkflow1.owner,
                                                  spec=testWorkflow1.spec)

        testWorkflow2 = Workflow(spec="spec2.xml", owner=owner,
                                 name="wf002", task="MultiUser-support")
        testWorkflow2.create()

        listFromOwner2 = loadFromOwnerDAO.execute(task=testWorkflow2.task,
                                                  dn=testWorkflow2.owner,
                                                  spec=testWorkflow2.spec)

        testWorkflow3 = Workflow(spec="spec3.xml", owner="Ciccio",
                                 name="wf003", task="MultiUser-support")
        testWorkflow3.create()

        listFromOwner3 = loadFromOwnerDAO.execute(task=testWorkflow3.task,
                                                  dn=testWorkflow3.owner,
                                                  spec=testWorkflow3.spec)

        self.assertEqual(testWorkflow1.owner, owner)
        self.assertEqual(listFromOwner1["owner"], owner)
        self.assertEqual(listFromOwner2["owner"], owner)
        self.assertEqual(listFromOwner1["owner"], listFromOwner2["owner"])
        self.assertNotEqual(listFromOwner1["owner"], listFromOwner3["owner"])
        self.assertNotEqual(listFromOwner2["owner"], listFromOwner3["owner"])
        return

    def testCountWorkflow(self):
        """
        _testCountWorkflow_

        """
        spec = "spec.py"
        owner = "moron"
        myThread = threading.currentThread()
        daoFactory = DAOFactory(package="WMCore.WMBS", logger=myThread.logger,
                                dbinterface=myThread.dbi)
        countSpecDAO = daoFactory(classname="Workflow.CountWorkflowBySpec")

        workflows = []
        for i in range(0, 10):
            testWorkflow = Workflow(spec=spec, owner=owner,
                                    name="wf00%i" % i, task="task%i" % i)
            testWorkflow.create()
            workflows.append(testWorkflow)

        self.assertEqual(countSpecDAO.execute(spec=spec), 10)

        for i in range(0, 10):
            wf = workflows.pop()
            wf.delete()
            self.assertEqual(countSpecDAO.execute(spec=spec), 10 - (i + 1))

        return

    def testWorkflowInjectMarking(self):
        """
        _testWorkflowInjectMarking_

        Test whether or not we can mark a workflow as injected or not.
        """
        owner = "moron"

        workflows = []
        for i in range(0, 10):
            testWorkflow = Workflow(spec="sp00%i" % i, owner=owner,
                                    name="wf00%i" % i, task="task%i" % i)
            testWorkflow.create()
            workflows.append(testWorkflow)

        myThread = threading.currentThread()
        daoFactory = DAOFactory(package="WMCore.WMBS", logger=myThread.logger,
                                dbinterface=myThread.dbi)
        getAction = daoFactory(classname="Workflow.GetInjectedWorkflows")
        markAction = daoFactory(classname="Workflow.MarkInjectedWorkflows")

        result = getAction.execute(injected=True)
        self.assertEqual(len(result), 0)
        result = getAction.execute(injected=False)
        self.assertEqual(len(result), 10)

        names = ['wf002', 'wf004', 'wf006', 'wf008']
        markAction.execute(names=names, injected=True)

        result = getAction.execute(injected=True)
        self.assertEqual(result, names)
        result = getAction.execute(injected=False)
        self.assertEqual(len(result), 6)
        return

    def testGetFinishedWorkflows(self):
        """
        _testGetFinishedWorkflows_

        Test that we get only those workflows which are finished, that is, workflows where
        all its subscriptions are finished and all other workflows with the same
        spec are finished too

        """

        owner = "no-one"

        # Create a bunch of worklows with "different" specs and tasks
        workflows = []
        for i in range(0, 100):
            scaledIndex = i % 10
            testWorkflow = Workflow(spec="sp00%i" % scaledIndex,
                                    owner=owner,
                                    name="wf00%i" % scaledIndex,
                                    task="task%i" % i)
            testWorkflow.create()
            workflows.append(testWorkflow)

        # Everyone will use this fileset
        testFileset = Fileset(name="TestFileset")
        testFileset.create()

        # Create subscriptions!
        subscriptions = []
        for workflow in workflows:
            subscription = Subscription(fileset=testFileset,
                                        workflow=workflow)
            subscription.create()
            subscriptions.append(subscription)

        # Check that all workflows are NOT finished
        myThread = threading.currentThread()
        daoFactory = DAOFactory(package="WMCore.WMBS", logger=myThread.logger,
                                dbinterface=myThread.dbi)

        getFinishedDAO = daoFactory(classname="Workflow.GetFinishedWorkflows")
        result = getFinishedDAO.execute()
        self.assertEqual(len(result), 0, "A workflow is incorrectly flagged as finished: %s" % str(result))

        # Mark the first 50 subscriptions as finished
        for idx, sub in enumerate(subscriptions):
            if idx > 49:
                break
            sub.markFinished()

        # No workflow is finished, none of them has all the subscriptions completed
        result = getFinishedDAO.execute()
        self.assertEqual(len(result), 0, "A workflow is incorrectly flagged as finished: %s" % str(result))

        # Now finish all workflows in wf{000-5}
        for idx, sub in enumerate(subscriptions):
            if idx < 50 or idx % 10 > 5:
                continue
            sub.markFinished()

        # Check the workflows
        result = getFinishedDAO.execute()
        self.assertEqual(len(result), 6, "A workflow is incorrectly flagged as finished: %s" % str(result))

        # Check the overall structure of the workflows
        for wf in result:
            # Sanity checks on the results
            # These are very specific checks and depends heavily on the names of task, spec and workflow
            self.assertEqual(wf[2:], result[wf]['spec'][2:],
                             "A workflow has the wrong spec-name combination: %s" % str(wf))
            self.assertTrue(int(wf[2:]) < 6,
                            "A workflow is incorrectly flagged as finished: %s" % str(wf))
            self.assertEqual(len(result[wf]['workflows']), 10,
                             "A workflow has more tasks than it should: %s" % str(result[wf]))
            for task in result[wf]['workflows']:
                self.assertEqual(len(result[wf]['workflows'][task]), 1,
                                 "A workflow has more subscriptions than it should: %s" % str(result[wf]))

        return


if __name__ == "__main__":
    unittest.main()
