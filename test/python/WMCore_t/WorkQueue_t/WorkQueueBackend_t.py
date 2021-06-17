#!/usr/bin/env python
"""
    CouchWorkQueueElement unit tests
"""
import unittest

import time

from Utils.PythonVersion import PY3
from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit
from WMCore.WorkQueue.WorkQueueBackend import WorkQueueBackend, sortAvailableElements
from WMCore.WorkQueue.DataStructs.CouchWorkQueueElement import CouchWorkQueueElement
from WMCore.WorkQueue.DataStructs.WorkQueueElement import WorkQueueElement

from WMCore.WMSpec.StdSpecs.ReReco import ReRecoWorkloadFactory
from WMQuality.Emulators.WMSpecGenerator.WMSpecGenerator import createConfig

rerecoArgs = ReRecoWorkloadFactory.getTestArguments()


def rerecoWorkload(workloadName, arguments):
    factory = ReRecoWorkloadFactory()
    wmspec = factory.factoryWorkloadConstruction(workloadName, arguments)
    return wmspec


class WorkQueueBackendTest(unittest.TestCase):
    def setUp(self):
        self.testInit = TestInit('CouchWorkQueueTest')
        self.testInit.setLogging()
        self.testInit.setupCouch('wq_backend_test_inbox', 'WorkQueue')
        self.testInit.setupCouch('wq_backend_test', 'WorkQueue')
        self.testInit.setupCouch('wq_backend_test_parent', 'WorkQueue')
        self.couch_db = self.testInit.couch.couchServer.connectDatabase('wq_backend_test')
        self.backend = WorkQueueBackend(db_url=self.testInit.couchUrl,
                                        db_name='wq_backend_test',
                                        inbox_name='wq_backend_test_inbox',
                                        parentQueue='%s/%s' % (self.testInit.couchUrl, 'wq_backend_test_parent'))
        rerecoArgs["ConfigCacheID"] = createConfig(rerecoArgs["CouchDBName"])
        self.processingSpec = rerecoWorkload('testProcessing', rerecoArgs)
        if PY3:
            self.assertItemsEqual = self.assertCountEqual

    def tearDown(self):
        """
        _tearDown_

        """
        self.testInit.tearDownCouch()

    def testPriority(self):
        """Element priority and ordering handled correctly"""
        element = WorkQueueElement(RequestName='backend_test',
                                   WMSpec=self.processingSpec,
                                   Status='Available',
                                   SiteWhitelist=["place"],
                                   Jobs=10, Priority=1)
        highprielement = WorkQueueElement(RequestName='backend_test_high',
                                          WMSpec=self.processingSpec,
                                          Status='Available', Jobs=10,
                                          SiteWhitelist=["place"],
                                          Priority=100)
        element2 = WorkQueueElement(RequestName='backend_test_2',
                                    WMSpec=self.processingSpec,
                                    Status='Available',
                                    SiteWhitelist=["place"],
                                    Jobs=10, Priority=1)
        element3 = WorkQueueElement(RequestName='backend_test_3',
                                    WMSpec=self.processingSpec,
                                    Status='Available',
                                    SiteWhitelist=["place"],
                                    Jobs=10, Priority=1)
        lowprielement = WorkQueueElement(RequestName='backend_test_low',
                                         WMSpec=self.processingSpec,
                                         Status='Available',
                                         SiteWhitelist=["place"],
                                         Jobs=10, Priority=0.1)
        self.backend.insertElements([element])
        self.backend.availableWork({'place': 1000}, {})
        # timestamp in elements have second coarseness, 2nd element must
        # have a higher timestamp to force it after the 1st
        time.sleep(1)
        self.backend.insertElements([lowprielement, element2, highprielement])
        self.backend.availableWork({'place': 1000}, {})
        time.sleep(1)
        self.backend.insertElements([element3])
        work = self.backend.availableWork({'place': 1000}, {})
        # order should be high to low, with the standard elements in the order
        # they were queueud
        self.assertEqual([x['RequestName'] for x in work[0]],
                         ['backend_test_high', 'backend_test', 'backend_test_2',
                          'backend_test_3', 'backend_test_low'])

    def testDuplicateInsertion(self):
        """Try to insert elements multiple times"""
        element1 = CouchWorkQueueElement(self.couch_db,
                                         elementParams={'RequestName': 'backend_test',
                                                        'WMSpec': self.processingSpec,
                                                        'Status': 'Available',
                                                        'Jobs': 10,
                                                        'Inputs': {
                                                            self.processingSpec.listInputDatasets()[0] + '#1': []}})
        element2 = CouchWorkQueueElement(self.couch_db,
                                         elementParams={'RequestName': 'backend_test',
                                                        'WMSpec': self.processingSpec,
                                                        'Status': 'Available',
                                                        'Jobs': 20,
                                                        'Inputs': {
                                                            self.processingSpec.listInputDatasets()[0] + '#2': []}})
        self.backend.insertElements([element1, element2])
        self.backend.insertElements([element1, element2])
        # check no duplicates and no conflicts
        self.assertEqual(len(self.backend.db.allDocs()['rows']), 4)  # design doc + workflow + 2 elements
        self.assertEqual(self.backend.db.loadView('WorkQueue', 'conflicts')['total_rows'], 0)

    def testSortAvailableElements(self):
        """Test logic used elemets sorting, in the fuction `sortAvailableElements`"""
        expected = [{'CreationTime': 2.4, 'Jobs': 2, 'Priority': 200},
                    {'CreationTime': 4.3, 'Jobs': 3, 'Priority': 200},
                    {'CreationTime': 1.5, 'Jobs': 5, 'Priority': 100},
                    {'CreationTime': 3.1, 'Jobs': 1, 'Priority': 100},
                    {'CreationTime': 5.2, 'Jobs': 4, 'Priority': 100}]

        elemList = [{'CreationTime': 3.1, 'Jobs': 1, 'Priority': 100},
                    {'CreationTime': 2.4, 'Jobs': 2, 'Priority': 200},
                    {'CreationTime': 4.3, 'Jobs': 3, 'Priority': 200},
                    {'CreationTime': 5.2, 'Jobs': 4, 'Priority': 100},
                    {'CreationTime': 1.5, 'Jobs': 5, 'Priority': 100}]
        sortAvailableElements(elemList)
        self.assertEqual(len(elemList), 5)
        self.assertItemsEqual(elemList, expected)


if __name__ == '__main__':
    unittest.main()
