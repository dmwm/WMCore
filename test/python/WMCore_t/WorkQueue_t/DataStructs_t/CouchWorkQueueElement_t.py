#!/usr/bin/env python
"""
    CouchWorkQueueElement unit tests
"""

import unittest
from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit

from WMCore.WorkQueue.DataStructs.CouchWorkQueueElement import CouchWorkQueueElement



class CouchWorkQueueElementTest(unittest.TestCase):

    def setUp(self):
        self.testInit = TestInit('CouchWorkQueueTest')
        self.testInit.setLogging()
        self.testInit.setupCouch('couch_wq_test')
        self.couch_db = self.testInit.couch.couchServer.connectDatabase('couch_wq_test')

    def tearDown(self):
        """
        _tearDown_

        """
        self.testInit.tearDownCouch()

    def testIdSaved(self):
        """Generated id used as db id"""
        ele = CouchWorkQueueElement(self.couch_db, elementParams = {'RequestName' : 'test'})
        ele.save()
        self.couch_db.commit(timestamp = True)
        self.assertTrue(self.couch_db.documentExists(ele.id))
        self.assertEqual(self.couch_db.info()['doc_count'], 1)

    def testIdFromDbImmutable(self):
        """Modifying element id algorithm doesn't change existing id's"""
        ele = CouchWorkQueueElement(self.couch_db, elementParams = {'RequestName' : 'test'})
        ele.save()
        self.couch_db.commit(timestamp = True)
        ele2 = CouchWorkQueueElement(self.couch_db, id = ele.id).load()
        ele2['RequestName'] = 'ThisWouldCauseIdToChange'
        # id should not change
        self.assertEqual(ele.id, ele2.id)
        # save should modify existing element
        ele2.save()
        self.couch_db.commit(timestamp = True)
        self.assertEqual(self.couch_db.info()['doc_count'], 1)


if __name__ == '__main__':
    unittest.main()
