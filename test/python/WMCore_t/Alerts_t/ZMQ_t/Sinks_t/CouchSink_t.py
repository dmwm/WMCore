#!/usr/bin/env python
# encoding: utf-8
"""
__init__.py

Created by Dave Evans on 2011-04-29.
Copyright (c) 2011 Fermilab. All rights reserved.
"""

import sys
import os
import unittest
import time
from WMCore.Alerts.ZMQ.Sinks.CouchSink import CouchSink
from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore.Alerts.Alert import Alert
from WMCore.Configuration import ConfigSection


class CouchSinkTests(unittest.TestCase):

    def setUp(self):
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setupCouch("couch_sink")
        
        self.config = ConfigSection("couch")
        self.config.url = self.testInit.couchUrl
        self.config.database = self.testInit.couchDbName

        
    def testA(self):
        
        sink = CouchSink(self.config)
        
        for i in range(10):
            a = Alert(Source=__file__, Level = i, Timestamp = time.time(), Type = "Test")
            sink.send(a)

        changes = sink.database.changes()
        self.assertEqual(len(changes[u'results']), 10)
        self.assertEqual(changes[u'last_seq'], 10)
        
        
        
        for i in range(10):
            a = Alert(Source=__file__, Level = i, Timestamp = time.time(), Type = "Test")
            sink.send(a)
        changes = sink.database.changes()
        self.assertEqual(len(changes[u'results']), 10)
        self.assertEqual(changes[u'last_seq'], 20)
        

    def tearDown(self):
        self.testInit.tearDownCouch()        
        return

if __name__ == '__main__':
    unittest.main()



