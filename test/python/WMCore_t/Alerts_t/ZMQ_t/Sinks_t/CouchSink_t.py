#!/usr/bin/env python
# encoding: utf-8

"""
Created by Dave Evans on 2011-04-29.
Copyright (c) 2011 Fermilab. All rights reserved.

"""

import time
import unittest

from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore.Configuration import ConfigSection
from WMCore.Alerts.Alert import Alert
from WMCore.Alerts.ZMQ.Sinks.CouchSink import CouchSink



class CouchSinkTest(unittest.TestCase):
    def setUp(self):
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        dbName = "couch_sink"
        self.testInit.setupCouch(dbName)

        self.config = ConfigSection("couch")
        self.config.url = self.testInit.couchUrl
        self.config.database = self.testInit.couchDbName


    def tearDown(self):
        self.testInit.tearDownCouch()


    def testCouchSinkBasic(self):
        sink = CouchSink(self.config)
        docIds = []
        for i in range(10):
            a = Alert(Source = __file__, Level = i, Timestamp = time.time(),
                      Type = "Test")
            retVals = sink.send([a])
            # return value is following format:
            # [{'rev': '1-ba0a0903d4d6ddcbb85ff64d48d8be14', 'id': 'b7e8f807c96f572418b39422ccea252c'}]
            # just 1 item was added in the list of alerts, so retVal is also 1 item list
            # and CMSCouch call commitOne also returns a list - hence second nesting
            docIds.append(retVals[0][0]["id"])
        changes = sink.database.changes()
        self.assertEqual(len(changes[u"results"]), 10)
        self.assertEqual(changes[u"last_seq"], 10)

        for i in range(10, 20):
            a = Alert(Source = __file__, Level = i, Timestamp = time.time(),
                      Type = "Test")
            retVals = sink.send([a])
            # just 1 item was added in the list of alerts, so retVal is also 1 item list
            # and CMSCouch call commitOne also returns a list - hence second nesting
            docIds.append(retVals[0][0]["id"])
        changes = sink.database.changes()
        self.assertEqual(len(changes[u"results"]), 10)
        self.assertEqual(changes[u"last_seq"], 20)

        # check documents presence
        for id, level in zip(docIds, range(20)):
            doc = sink.database.document(id)
            self.assertEqual(doc["Level"], level)



if __name__ == "__main__":
    unittest.main()
