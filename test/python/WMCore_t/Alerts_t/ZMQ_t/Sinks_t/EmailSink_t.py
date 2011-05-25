#!/usr/bin/env python
# encoding: utf-8
"""
EmailSink.py

Created by Dave Evans on 2011-04-29.
Copyright (c) 2011 Fermilab. All rights reserved.
"""

import sys
import os

import unittest
import time
from WMCore.Alerts.Alert import Alert
from WMCore.Configuration import ConfigSection
from WMCore.Alerts.ZMQ.Sinks.EmailSink import EmailSink


class EmailSinkTests(unittest.TestCase):
    def setUp(self):
        self.config = ConfigSection("email")
        self.config.fromAddr = "sfoulkes@fnal.gov"
        # very easy to make this unittest get real spammy...
        self.config.toAddr = ["sfoulkes@fnal.gov", "mnorman@fnal.gov", "meloam@fnal.gov"]
        self.config.smtpServer = "smtp.fnal.gov"
        self.config.smtpUser = None
        self.config.smtpPass = None
        
    def testA(self):
        sink = EmailSink(self.config)
        
        alerts = []
        for i in range(10):
            alerts.append(Alert(Source=__file__, Level = i, Timestamp = time.time(), Type = "Test"))
        # UNCOMMENT TO ACTUALLY SEND MAILS
        #sink.send(alerts)


if __name__ == '__main__':
    unittest.main()