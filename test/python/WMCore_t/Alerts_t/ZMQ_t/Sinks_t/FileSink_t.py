#!/usr/bin/env python
# encoding: utf-8
"""
FileSink_t.py

Created by Dave Evans on 2011-04-29.
Copyright (c) 2011 Fermilab. All rights reserved.
"""

import sys
import os
import unittest
import time
import json
from WMCore.Alerts.Alert import Alert
from WMCore.Configuration import ConfigSection
from WMCore.Alerts.ZMQ.Sinks.FileSink import FileSink

class FileSinkTests(unittest.TestCase):
    def setUp(self):
        self.config = ConfigSection('file')
        self.config.outputfile = "/tmp/FileSink.json"
        self.config.depth = 100
    
    def tearDown(self):
        if os.path.exists(self.config.outputfile):
            os.remove(self.config.outputfile)
    
    def testA(self):
        
        sink = FileSink(self.config)
        alerts = []
        for i in range(10):
            alerts.append(Alert(Source=__file__, Level = i, Timestamp = time.time(), Type = "Test"))

        sink.send(alerts)
        self.failUnless(os.path.exists(self.config.outputfile))
        handle = open(self.config.outputfile, 'r')
        self.assertEqual(len(list(json.load(handle))), 10)
        handle.close()
        alerts = []
        for i in range(10,20):
            alerts.append(Alert(Source=__file__, Level = i, Timestamp = time.time(), Type = "Test"))

        sink.send(alerts)
        self.failUnless(os.path.exists(self.config.outputfile))
        handle = open(self.config.outputfile, 'r')
        self.assertEqual(len(list(json.load(handle))), 20)
        handle.close()
        

if __name__ == '__main__':
    unittest.main()