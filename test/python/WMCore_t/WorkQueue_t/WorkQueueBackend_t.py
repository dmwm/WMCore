#!/usr/bin/env python
"""
    CouchWorkQueueElement unit tests
"""

import unittest
import time
from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit

from WMCore.WorkQueue.WorkQueueBackend import WorkQueueBackend
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
        self.couch_db = self.testInit.couch.couchServer.connectDatabase('wq_backend_test')
        self.backend = WorkQueueBackend(db_url = self.testInit.couchUrl,
                                        db_name = 'wq_backend_test',
                                        inbox_name = 'wq_backend_test_inbox')

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
        self.backend.availableWork({'place' : 1000})
        # timestamp in elements have second coarseness, 2nd element must
        # have a higher timestamp to force it after the 1st
        time.sleep(1)
        self.backend.insertElements([lowprielement, element2, highprielement])
        self.backend.availableWork({'place' : 1000})
        work = self.backend.availableWork({'place' : 1000})
        # order should be high to low, with the standard elements in the order
        # they were queueud
        #import pdb; pdb.set_trace()
        self.assertEqual([x['RequestName'] for x in work[0]],
                         ['backend_test_high', 'backend_test', 'backend_test_2', 'backend_test_low'])


if __name__ == '__main__':
    unittest.main()
