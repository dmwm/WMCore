#!/usr/bin/env python
"""
_WorkUnit_t_

Unittest for the WMCore.DataStructs.WorkUnit class

"""

from __future__ import absolute_import, division, print_function

import json
import unittest

from Utils.PythonVersion import PY3

from WMCore.DataStructs.Run import Run
from WMCore.DataStructs.WorkUnit import WorkUnit

TEST_TASKID = 1
TEST_LAST_UNIT_COUNT = 20
TEST_FILEID = 101
TEST_LUMI = 1
TEST_RUN_NUMBER = 1000000


class WorkUnitTest(unittest.TestCase):
    """
    _WorkUnitTest_
    """
    def setUp(self):
        if PY3:
            self.assertItemsEqual = self.assertCountEqual

    def testWorkUnitDefinitionDefault(self):
        """
        This tests the construction of a DataStructs WorkUnit object
        """

        testRunLumi = Run(TEST_RUN_NUMBER, TEST_LUMI)
        testWorkUnit = WorkUnit(taskID=TEST_TASKID, lastUnitCount=TEST_LAST_UNIT_COUNT, fileid=TEST_FILEID,
                                runLumi=testRunLumi)

        # Test the things we set
        self.assertEqual(testWorkUnit['taskid'], TEST_TASKID)
        self.assertEqual(testWorkUnit['last_unit_count'], TEST_LAST_UNIT_COUNT)
        self.assertEqual(testWorkUnit['fileid'], TEST_FILEID)
        self.assertEqual(testWorkUnit['run_lumi'].run, TEST_RUN_NUMBER)
        self.assertCountEqual(testWorkUnit['run_lumi'].lumis, [TEST_LUMI]) if PY3 else self.assertItemsEqual(testWorkUnit['run_lumi'].lumis, [TEST_LUMI])

        # Test the defaults we did not set
        self.assertEqual(testWorkUnit['retry_count'], 0)
        self.assertGreater(testWorkUnit['last_submit_time'], 0)
        self.assertEqual(testWorkUnit['status'], 0)
        self.assertGreaterEqual(testWorkUnit['firstevent'], 0)
        self.assertGreaterEqual(testWorkUnit['lastevent'], 0)

        return

    def testWorkUnitDefinitionNotDefault(self):
        """
        This tests the construction of a DataStructs WorkUnit object with non-default values
        """

        # Things with default values
        testRetries = 10
        testSubmitTime = 10 * 365 * 24 * 3600
        testStatus = 4
        testFirstEvent = 100
        testLastEvent = 600

        testRunLumi = Run(TEST_RUN_NUMBER, TEST_LUMI)
        testWorkUnit = WorkUnit(taskID=TEST_TASKID, lastUnitCount=TEST_LAST_UNIT_COUNT, fileid=TEST_FILEID,
                                runLumi=testRunLumi, retryCount=testRetries, lastSubmitTime=testSubmitTime,
                                status=testStatus, firstEvent=testFirstEvent, lastEvent=testLastEvent)

        # Test the defaults we overrode
        self.assertEqual(testWorkUnit['retry_count'], testRetries)
        self.assertEqual(testWorkUnit['last_submit_time'], testSubmitTime)
        self.assertEqual(testWorkUnit['status'], testStatus)
        self.assertEqual(testWorkUnit['firstevent'], testFirstEvent)
        self.assertEqual(testWorkUnit['lastevent'], testLastEvent)

        return

    def testWorkUnitHashAndCompare(self):
        """
        Test that the hash function works and that the comparisons work
        """

        testRunLumi0 = Run(TEST_RUN_NUMBER, TEST_LUMI)
        testWorkUnit0 = WorkUnit(taskID=TEST_TASKID, lastUnitCount=TEST_LAST_UNIT_COUNT, fileid=TEST_FILEID,
                                 runLumi=testRunLumi0)
        testRunLumi1 = Run(TEST_RUN_NUMBER, TEST_LUMI)
        testWorkUnit1 = WorkUnit(taskID=TEST_TASKID, lastUnitCount=TEST_LAST_UNIT_COUNT, fileid=TEST_FILEID,
                                 runLumi=testRunLumi1)
        testRunLumi2 = Run(TEST_RUN_NUMBER, TEST_LUMI + 1)
        testWorkUnit2 = WorkUnit(taskID=TEST_TASKID, lastUnitCount=TEST_LAST_UNIT_COUNT, fileid=TEST_FILEID,
                                 runLumi=testRunLumi2)
        testRunLumi3 = Run(TEST_RUN_NUMBER + 1, TEST_LUMI)
        testWorkUnit3 = WorkUnit(taskID=TEST_TASKID, lastUnitCount=TEST_LAST_UNIT_COUNT, fileid=TEST_FILEID,
                                 runLumi=testRunLumi3)

        # Tests for hashers
        self.assertEqual(hash(testWorkUnit0), hash(testWorkUnit1))
        self.assertNotEqual(hash(testWorkUnit1), hash(testWorkUnit2))
        self.assertNotEqual(hash(testWorkUnit1), hash(testWorkUnit3))

        # Tests for comparisons
        self.assertEqual(testWorkUnit0, testWorkUnit1)
        self.assertLess(testWorkUnit1, testWorkUnit2)
        self.assertLess(testWorkUnit2, testWorkUnit3)

        return

    def testWorkUnitJsonable(self):
        """
        Test that the object can be turned into JSON
        """

        testRunLumi = Run(TEST_RUN_NUMBER, TEST_LUMI)
        testWorkUnit = WorkUnit(taskID=TEST_TASKID, lastUnitCount=TEST_LAST_UNIT_COUNT, fileid=TEST_FILEID,
                                runLumi=testRunLumi)

        self.assertTrue(json.dumps(testWorkUnit.__to_json__()))

    def testGetInfo(self):
        testRunLumi = Run(TEST_RUN_NUMBER, TEST_LUMI)
        testWorkUnit = WorkUnit(taskID=TEST_TASKID, lastUnitCount=TEST_LAST_UNIT_COUNT, fileid=TEST_FILEID,
                                runLumi=testRunLumi)

        info = testWorkUnit.getInfo()

        # Test the things we set
        self.assertEqual(info[0], TEST_TASKID)
        self.assertEqual(info[2], TEST_LAST_UNIT_COUNT)
        self.assertEqual(info[7], TEST_FILEID)
        self.assertEqual(info[8].run, TEST_RUN_NUMBER)
        self.assertCountEqual(info[8].lumis, [TEST_LUMI]) if PY3 else self.assertItemsEqual(info[8].lumis, [TEST_LUMI])

        # Test the defaults we did not set
        self.assertEqual(info[1], 0)
        self.assertGreater(info[3], 0)
        self.assertEqual(info[4], 0)
        self.assertGreaterEqual(info[5], 0)
        self.assertGreaterEqual(info[6], 0)

        # Run another test by overriding defaults of things with default values
        testRetries = 10
        testSubmitTime = 10 * 365 * 24 * 3600
        testStatus = 4
        testFirstEvent = 100
        testLastEvent = 600

        testWorkUnit = WorkUnit(taskID=TEST_TASKID, lastUnitCount=TEST_LAST_UNIT_COUNT, fileid=TEST_FILEID,
                                runLumi=testRunLumi, retryCount=testRetries, lastSubmitTime=testSubmitTime,
                                status=testStatus, firstEvent=testFirstEvent, lastEvent=testLastEvent)
        info = testWorkUnit.getInfo()

        # Test the defaults we overrode
        self.assertEqual(info[1], testRetries)
        self.assertEqual(info[3], testSubmitTime)
        self.assertEqual(info[4], testStatus)
        self.assertEqual(info[5], testFirstEvent)
        self.assertEqual(info[6], testLastEvent)

        return


if __name__ == '__main__':
    unittest.main()
