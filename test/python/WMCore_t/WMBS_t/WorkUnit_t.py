#!/usr/bin/env python
"""
_WorkUnit_t_

Unit tests for the WMBS WorkUnit class.
"""

from __future__ import absolute_import, division, print_function

import threading
import unittest

from WMCore.DAOFactory import DAOFactory
from WMCore.DataStructs.Run import Run
from WMCore.DataStructs.WorkUnit import WU_STATES, JOB_WU_STATE_MAP
from WMCore.JobStateMachine.Transitions import Transitions
from WMCore.WMBS.File import File
from WMCore.WMBS.WorkUnit import WorkUnit
from WMCore.WMBS.Workflow import Workflow
from WMQuality.TestInit import TestInit

WF_NAME = 'Test'


class WMBSWorkUnitTest(unittest.TestCase):
    """
    _WorkUnit_t_

    Unit tests for the WMBS WorkUnit class.
    """

    def __init__(self, *args, **kwargs):
        super(WMBSWorkUnitTest, self).__init__(*args, **kwargs)
        self.testWorkflow = None
        self.testFile = None

    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Try to create all of the
        WMBS tables.
        """

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules=["WMCore.WMBS"], useDefault=False)

        myThread = threading.currentThread()
        self.daofactory = DAOFactory(package="WMCore.WMBS", logger=myThread.logger, dbinterface=myThread.dbi)

        self.createPrerequisites()

        return

    def tearDown(self):
        """
        _tearDown_

        Drop all the WMBS tables.
        """
        self.testInit.clearDatabase()
        return

    def createPrerequisites(self):
        """
        Create a dummy workflow we can use later
        """

        self.testWorkflow = Workflow(spec="spec.xml", owner="Simon", name="wf001", task=WF_NAME, wfType="ReReco")
        self.testWorkflow.create()

        self.testFile = File(lfn="/this/is/a/test/file0", size=1024, events=10)
        self.testFile.addRun(Run(1, *[44]))
        self.testFile.create()

        return

    def testCreateDeleteExists(self):
        """
        _testCreateDeleteExists_

        Test the create(), delete() and exists() methods of the WorkUnit class
        by creating and deleting a WorkUnit.  The exists() method will be
        called before and after creation and after deletion.
        """

        testWU = WorkUnit(wuid=1, taskID=self.testWorkflow.id, lastUnitCount=1, fileid=1, runLumi=Run(1, *[44]))
        sameWU = WorkUnit(taskID=self.testWorkflow.id, lastUnitCount=1, fileid=1, runLumi=Run(1, *[44]))

        self.assertFalse(testWU.exists(), "WorkUnit exists before it was created")
        self.assertFalse(sameWU.exists(), "WorkUnit exists before it was created")

        testWU.create()
        self.assertTrue(testWU.exists(), "WorkUnit does not exist after it was created")
        self.assertTrue(sameWU.exists(), "WorkUnit does not exist after it was created")

        testWU.delete()
        self.assertFalse(testWU.exists(), "WorkUnit exists after it has been deleted")
        self.assertFalse(sameWU.exists(), "WorkUnit exists after it has been deleted")

        return

    def testCreateTransaction(self):
        """
        _testCreateTransaction_

        Begin a transaction and then create a WorkUnit in the database.  Afterwards,
        rollback the transaction.  Use the WorkUnit class's exists() method to
        to verify that the file doesn't exist before it was created, exists
        after it was created and doesn't exist after the transaction was rolled
        back.
        """

        myThread = threading.currentThread()
        myThread.transaction.begin()

        testWU = WorkUnit(wuid=1, taskID=self.testWorkflow.id, lastUnitCount=1, fileid=1, runLumi=Run(1, *[44]))

        self.assertFalse(testWU.exists(), "WorkUnit exists before it was created")

        testWU.create()
        self.assertTrue(testWU.exists(), "WorkUnit does not exist after it was created")

        myThread.transaction.rollback()
        self.assertFalse(testWU.exists(), "WorkUnit exists after rollback")

        return

    def testDeleteTransaction(self):
        """
        _testDeleteTransaction_

        Create a WorkUnit and commit it to the database.  Start a new transaction
        and delete the WorkUnit.  Rollback the transaction after the WorkUnit has been
        deleted.  Use the WorkUnit class's exists() method to verify that the file
        does not exist after it has been deleted but does exist after the
        transaction is rolled back.
        """

        testWU = WorkUnit(wuid=1, taskID=self.testWorkflow.id, lastUnitCount=1, fileid=1, runLumi=Run(1, *[44]))

        self.assertFalse(testWU.exists(), "WorkUnit exists before it was created")

        testWU.create()
        self.assertTrue(testWU.exists(), "WorkUnit does not exist after it was created")

        myThread = threading.currentThread()
        myThread.transaction.begin()

        testWU.delete()
        self.assertFalse(testWU.exists(), "WorkUnit exists after it was deleted")

        myThread.transaction.rollback()
        self.assertTrue(testWU.exists(), "WorkUnit does not exist transaction was rolled back")

        return

    def testGetInfo(self):
        """
        _testGetInfo_

        Test the getInfo() method of the WorkUnit class to make sure that it
        returns the correct id. Everything else is tested by the tests in
        DataStructs_t which test the underlying getInfo()
        """

        testWU = WorkUnit(wuid=1, taskID=self.testWorkflow.id, lastUnitCount=1, fileid=1, runLumi=Run(1, *[44]))
        testWU.create()
        info = testWU.getInfo()
        self.assertEqual(info[0], testWU['id'], "Work unit returned wrong ID")

        return

    def testLoad(self):
        """
        _testLoad_

        Test the loading of WorkUnit data using the ID of a WorkUnit
        """

        testRetries = 10
        testSubmitTime = 10 * 365 * 24 * 3600
        testStatus = 4
        testFirstEvent = 100
        testLastEvent = 600
        testLastUnitCount = 4
        testRunLumi = Run(1, 44)

        intFields = ['id', 'taskid', 'retry_count', 'last_unit_count', 'last_submit_time', 'status',
                     'fileid', 'firstevent', 'lastevent']
        allFields = intFields + ['run_lumi']

        testWU = WorkUnit(wuid=1, taskID=self.testWorkflow.id, lastUnitCount=testLastUnitCount,
                          fileid=self.testFile['id'],
                          runLumi=testRunLumi, retryCount=testRetries, lastSubmitTime=testSubmitTime,
                          status=testStatus, firstEvent=testFirstEvent, lastEvent=testLastEvent)
        testWU.create()

        loadedWU = WorkUnit(wuid=testWU['id'])
        loadedWU.load()

        # Make sure load() gets the type set correctly
        for field in intFields:
            self.assertIsInstance(loadedWU[field], int, 'Field "%s" is not of type int' % field)

        # Make sure the values com back OK
        for field in allFields:
            self.assertEqual(testWU[field], loadedWU[field],
                             'Field "%s" is not returned correctly (got %s instead of %s)' %
                             (field, loadedWU[field], testWU[field]))

        # Now load things by taskid, fileid, run/lumi and run the same tests

        loadByFRL = WorkUnit(taskID=self.testWorkflow.id, fileid=self.testFile['id'], runLumi=testRunLumi)
        loadByFRL.load()

        # Make sure load() gets the type set correctly
        for field in intFields:
            self.assertIsInstance(loadByFRL[field], int, 'Field "%s" is not of type int' % field)

        # Make sure the values com back OK
        for field in allFields:
            self.assertEqual(testWU[field], loadByFRL[field],
                             'Field "%s" is not returned correctly (got %s instead of %s)' %
                             (field, loadByFRL[field], testWU[field]))
        testWU.delete()
        return

    def testState(self):
        """
        _testChangeState_

        Test the getState and changeState methods
        """

        testWU = WorkUnit(taskID=self.testWorkflow.id, lastUnitCount=1, fileid=1, runLumi=Run(1, *[44]))
        testWU.create()
        testWU.load()
        self.assertEqual(testWU.getState(), WU_STATES['new'])

        testWU.delete()
        self.assertFalse(testWU.exists(), "WorkUnit exists after it has been deleted")

        return

    def testStateList(self):
        """
        _testStateList_

        Make sure all states in the job state machine have a correpsonding state in work unit
        """

        transitions = Transitions()
        jobStates = transitions.states()
        wuStates = list(JOB_WU_STATE_MAP.keys())
        for jobState in jobStates:
            self.assertIn(jobState, wuStates)


if __name__ == "__main__":
    unittest.main()
