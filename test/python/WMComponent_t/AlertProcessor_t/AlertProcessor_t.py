#!/usr/bin/env python
# encoding: utf-8
"""
AlertProcessor_t.py

Created by Dave Evans on 2011-03-21.
Copyright (c) 2011 Fermilab. All rights reserved.
"""

import unittest
from WMQuality.TestInit import TestInit
from WMCore.Configuration import Configuration
from WMComponent.AlertProcessor.AlertProcessor import AlertProcessor as AlertProc

class AlertProcessor_t(unittest.TestCase):

    def setUp(self):


        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS",'WMCore.Agent.Database',
                                                 "WMCore.ResourceControl"],
                                 useDefault = False)
        self.testDir = self.testInit.generateWorkDir()
        self.config = Configuration()
        self.config.section_("Agent")
        self.config.Agent.useMsgService = False
        self.config.Agent.useTrigger = False
        self.config.component_("AlertProcessor")
        self.config.AlertProcessor.componentDir = self.testDir
        self.config.AlertProcessor.processorListensOn = "tcp://127.0.0.1:5557"
        self.config.AlertProcessor.processorControlOn = "tcp://127.0.0.1:5559"
        self.config.section_("CoreDatabase")
        self.config.CoreDatabase.socket = '/tmp/mysql.sock'
        self.config.CoreDatabase.connectUrl = "mysql://evansde:Telecast3r@localhost:3309/WMAgent"



    def testA(self):

        proc = AlertProc(self.config)
        proc.startComponent()

    def tearDown(self):
        self.testInit.clearDatabase()        
        self.testInit.delWorkDir()


    
if __name__ == '__main__':
	unittest.main()