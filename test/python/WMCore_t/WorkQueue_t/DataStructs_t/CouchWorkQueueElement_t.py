#!/usr/bin/env python
"""
    CouchWorkQueueElement unit tests
"""

import unittest
from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit

from WMCore.WorkQueue.DataStructs.CouchWorkQueueElement import CouchWorkQueueElement, fixElementConflicts



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

    def testFixElementConflicts(self):
        """Element conflicts handled"""
        # list of before and after elements for comparison
        values = [
                  # status conflict
                  [[{'RequestName' : 'arequest', 'Status' : 'Available'}, {'RequestName' : 'arequest', 'Status' : 'CancelRequested'}],
                  [{'RequestName' : 'arequest', 'Status' : 'CancelRequested'}, {'RequestName' : 'arequest', 'Status' : 'CancelRequested'}]
                 ],
                  # location conflict - local uses location and global subscriptions
                  [[{'RequestName' : 'brequest', 'Status' : 'CancelRequested', 'Inputs' : {'a' : [1]}}, {'RequestName' : 'brequest', 'Status' : 'Available', 'Inputs' : {'a' : [1,2]}}],
                  [{'RequestName' : 'brequest', 'Status' : 'CancelRequested', 'Inputs' : {'a' : [1,2]}}, {'RequestName' : 'brequest', 'Status' : 'Available', 'Inputs' : {'a' : [1,2]}}]
                 ],
                  # status and progress conflict
                  [[{'RequestName' : 'crequest', 'Status' : 'Available', 'PercentComplete' : 69}, {'RequestName' : 'crequest', 'Status' : 'CancelRequested'}],
                  [{'RequestName' : 'crequest', 'Status' : 'CancelRequested', 'PercentComplete' : 69}, {'RequestName' : 'crequest', 'Status' : 'CancelRequested'}]
                 ],
                  # status and subscription conflict
                  [[{'RequestName' : 'drequest', 'Status' : 'Running', 'SubscriptionId' : 69}, {'RequestName' : 'drequest', 'Status' : 'CancelRequested'}],
                  [{'RequestName' : 'drequest', 'Status' : 'CancelRequested', 'SubscriptionId' : 69}, {'RequestName' : 'drequest', 'Status' : 'CancelRequested'}]
                 ],
                  # whitelist conflict
                  [[{'RequestName' : 'erequest', 'SiteWhitelist' : [1,2]}, {'RequestName' : 'erequest'}],
                  [{'RequestName' : 'erequest', 'SiteWhitelist' : [1,2]}, {'RequestName' : 'erequest'}]
                 ],
                  # black conflict
                  [[{'RequestName' : 'frequest'}, {'RequestName' : 'frequest', 'SiteBlacklist' : [1,2]}],
                  [{'RequestName' : 'frequest', 'SiteBlacklist' : [1,2]}, {'RequestName' : 'frequest', 'SiteBlacklist' : [1,2]}]
                 ],
                  # priority conflict
                  [[{'RequestName' : 'grequest'}, {'RequestName' : 'grequest', 'Priority' : 69}],
                  [{'RequestName' : 'grequest', 'Priority' : 69}, {'RequestName' : 'grequest', 'Priority' : 69}]
                 ],
                ]
        for before, after in values:
            before = [CouchWorkQueueElement(self.couch_db, 1, elementParams = x) for x in before]
            # add fake revs
            [x._document.__setitem__('_rev', "a") for x in before]
            after = [CouchWorkQueueElement(self.couch_db, 1, elementParams = x) for x in after]
            self.assertEqual(list(fixElementConflicts(*before)), after)

if __name__ == '__main__':
    unittest.main()
