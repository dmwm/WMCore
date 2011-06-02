#!/usr/bin/env python
# encoding: utf-8

"""
Created by Dave Evans on 2011-04-29.
Copyright (c) 2011 Fermilab. All rights reserved.

"""

import time
import unittest
import smtplib

from minimock import Mock

from WMCore.Configuration import ConfigSection
from WMCore.Alerts.Alert import Alert
from WMCore.Alerts.ZMQ.Sinks.EmailSink import EmailSink



class EmailSinkTest(unittest.TestCase):
    def setUp(self):
        self.config = ConfigSection("email")
        self.config.fromAddr = "sfoulkes@fnal.gov"
        self.config.toAddr = ["sfoulkes@fnal.gov", "mnorman@fnal.gov", "meloam@fnal.gov"]
        self.config.smtpServer = "smtp.fnal.gov"
        self.config.smtpUser = None
        self.config.smtpPass = None
        
        # now we want to make a mock smtplib.SMTP object, have to inject
        # our mock into the smtplib module, otherwise with the above
        # configuration, the test will send out too much spam 
        smtplib.SMTP = Mock("smtplib.SMTP")
        smtplib.SMTP.mock_returns = Mock("smtp_connection")
        
        
    def testEmailSinkBasic(self):
        sink = EmailSink(self.config)
        alerts = []
        for i in range(10):
            a = Alert(Source=__file__, Level = i, Timestamp = time.time(), Type = "Test")
            alerts.append(a)
        sink.send(alerts)



if __name__ == "__main__":
    unittest.main()