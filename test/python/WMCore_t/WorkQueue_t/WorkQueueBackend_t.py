#!/usr/bin/env python
"""
    CouchWorkQueueElement unit tests
"""

import unittest
import time
from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit

from WMCore.WorkQueue.WorkQueueBackend import WorkQueueBackend
from WMCore.WorkQueue.DataStructs.CouchWorkQueueElement import CouchWorkQueueElement
from WMCore.WorkQueue.DataStructs.WorkQueueElement import WorkQueueElement

from WMCore.WMSpec.StdSpecs.ReReco import rerecoWorkload as rerecoWMSpec, \
                                          getTestArguments as getRerecoArgs

rerecoArgs = getRerecoArgs()
def rerecoWorkload(workloadName, arguments):
    wmspec = rerecoWMSpec(workloadName, arguments)
    return wmspec

class WorkQueueBackendTest(unittest.TestCase):

    def setUp(self):
        self.testInit = TestInit('CouchWorkQueueTest')
        self.testInit.setLogging()
        self.testInit.setupCouch('wq_backend_test_inbox', 'WorkQueue')
        self.testInit.setupCouch('wq_backend_test', 'WorkQueue')
        self.testInit.setupCouch('wq_backend_test_parent', 'WorkQueue')
        self.couch_db = self.testInit.couch.couchServer.connectDatabase('wq_backend_test')
        self.backend = WorkQueueBackend(db_url = self.testInit.couchUrl,
                                        db_name = 'wq_backend_test',
                                        inbox_name = 'wq_backend_test_inbox',
                                        parentQueue = '%s/%s' % (self.testInit.couchUrl, 'wq_backend_test_parent'))

        self.processingSpec = rerecoWorkload('testProcessing', rerecoArgs)


    def tearDown(self):
        """
        _tearDown_

        """
        self.testInit.tearDownCouch()

    def testPriority(self):
        """Element priority and ordering handled correctly"""
        element = WorkQueueElement(RequestName = 'backend_test',
                                   WMSpec = self.processingSpec,
                                   Status = 'Available',
                                   Jobs = 10, Priority = 1)
        highprielement = WorkQueueElement(RequestName = 'backend_test_high',
                                          WMSpec = self.processingSpec,
                                          Status = 'Available', Jobs = 10,
                                          Priority = 100)
        element2 = WorkQueueElement(RequestName = 'backend_test_2',
                                    WMSpec = self.processingSpec,
                                    Status = 'Available',
                                    Jobs = 10, Priority = 1)
        lowprielement = WorkQueueElement(RequestName = 'backend_test_low',
                                         WMSpec = self.processingSpec,
                                         Status = 'Available',
                                         Jobs = 10, Priority = 0.1)
        self.backend.insertElements([element])
        self.backend.availableWork({'place' : 1000}, {})
        # timestamp in elements have second coarseness, 2nd element must
        # have a higher timestamp to force it after the 1st
        time.sleep(1)
        self.backend.insertElements([lowprielement, element2, highprielement])
        self.backend.availableWork({'place' : 1000}, {})
        work = self.backend.availableWork({'place' : 1000}, {})
        # order should be high to low, with the standard elements in the order
        # they were queueud
        self.assertEqual([x['RequestName'] for x in work[0]],
                         ['backend_test_high', 'backend_test', 'backend_test_2', 'backend_test_low'])


    def testDuplicateInsertion(self):
        """Try to insert elements multiple times"""
        element1 = CouchWorkQueueElement(self.couch_db,
                                         elementParams = {'RequestName' : 'backend_test',
                                                          'WMSpec' : self.processingSpec,
                                                          'Status' : 'Available',
                                                          'Jobs' : 10,
                                                          'Inputs' : {self.processingSpec.listInputDatasets()[0] + '#1' : []}})
        element2 = CouchWorkQueueElement(self.couch_db,
                                         elementParams = {'RequestName' : 'backend_test',
                                                          'WMSpec' : self.processingSpec,
                                                          'Status' : 'Available',
                                                          'Jobs' : 20,
                                                          'Inputs' : {self.processingSpec.listInputDatasets()[0] + '#2' : []}})
        self.backend.insertElements([element1, element2])
        self.backend.insertElements([element1, element2])
        # check no duplicates and no conflicts
        self.assertEqual(len(self.backend.db.allDocs()['rows']), 4) # design doc + workflow + 2 elements
        self.assertEqual(self.backend.db.loadView('WorkQueue', 'conflicts')['total_rows'], 0)

    def testReplicationStatus(self):
        """
        _testReplicationStatus_

        Check that we can catch replication errors,
        the checkReplicationStatus returns True if there is no error.
        """
        self.backend.pullFromParent(continuous = True)
        self.backend.sendToParent(continuous = True)
        self.assertTrue(self.backend.checkReplicationStatus())
        self.backend.pullFromParent(continuous = True, cancel = True)
        self.backend.sendToParent(continuous = True, cancel = True)
        self.assertFalse(self.backend.checkReplicationStatus())
        self.backend.pullFromParent(continuous = True)
        self.backend.sendToParent(continuous = True)
        self.assertTrue(self.backend.checkReplicationStatus())

if __name__ == '__main__':
    unittest.main()
