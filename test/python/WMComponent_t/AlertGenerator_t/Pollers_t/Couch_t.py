"""
Tests for all CouchDb related metrics pollers for the Alerts messaging
framework.

"""

import os
import unittest
import logging
import types
import multiprocessing
import random
import time

from WMQuality.TestInit import TestInit
from WMComponent_t.AlertGenerator_t.AlertGenerator_t import getConfig
from WMComponent_t.AlertGenerator_t.Pollers_t import utils
from WMComponent.AlertGenerator.Pollers.Couch import CouchPoller
from WMComponent.AlertGenerator.Pollers.Couch import CouchDbSizePoller
from WMComponent.AlertGenerator.Pollers.Couch import CouchCPUPoller
from WMComponent.AlertGenerator.Pollers.Couch import CouchMemoryPoller



class CouchTest(unittest.TestCase):
    def setUp(self):
        self.testInit = TestInit(__file__)
        # still no effect, .debug, .info not appearing ...
        self.testInit.setLogging(logLevel = logging.NOTSET)
        self.testDir = self.testInit.generateWorkDir()
        self.config = getConfig(self.testDir)
        # mock generator instance to communicate some configuration values
        self.generator = utils.AlertGeneratorMock(self.config)        
        self.testProcesses = []
        self.testName = self.id().split('.')[-1]
         
        
    def tearDown(self):       
        self.testInit.delWorkDir()
        self.generator = None
        utils.terminateProcesses(self.testProcesses)


    def testCouchDbSizePollerBasic(self):
        config = getConfig("/tmp")
        try:
            poller = CouchDbSizePoller(config.AlertGenerator.couchDbSizePoller, self.generator)
        except Exception, ex:
            self.fail("%s: exception: %s" % (self.testName, ex))
        poller.check() # -> on real system dir may result in permission denied
        poller._dbDirectory = "/dev"
        poller.check() # -> OK

        
    def testAlertGeneratorCouchDbSizePollerSoftThreshold(self):
        self.config.AlertGenerator.couchDbSizePoller.soft = 5
        self.config.AlertGenerator.couchDbSizePoller.critical = 10
        self.config.AlertGenerator.couchDbSizePoller.pollInterval = 0.2
        ti = utils.TestInput() # see attributes comments at the class
        ti.pollerClass = CouchDbSizePoller
        ti.config = self.config.AlertGenerator.couchDbSizePoller
        ti.thresholdToTest = self.config.AlertGenerator.couchDbSizePoller.soft
        ti.level = self.config.AlertProcessor.all.level
        ti.expected = 1
        ti.thresholdDiff = 4
        ti.testCase = self
        utils.doDirectorySizePolling(ti)
        

    def testAlertGeneratorCouchDbSizePollerCriticalThreshold(self):
        self.config.AlertGenerator.couchDbSizePoller.soft = 5
        self.config.AlertGenerator.couchDbSizePoller.critical = 10
        self.config.AlertGenerator.couchDbSizePoller.pollInterval = 0.2        
        ti = utils.TestInput() # see attributes comments at the class
        ti.pollerClass = CouchDbSizePoller
        ti.config = self.config.AlertGenerator.couchDbSizePoller
        ti.thresholdToTest = self.config.AlertGenerator.couchDbSizePoller.critical
        ti.level = self.config.AlertProcessor.critical.level
        ti.expected = 1
        ti.thresholdDiff = 1
        ti.testCase = self
        utils.doDirectorySizePolling(ti)
        

    def testAlertGeneratorCouchDbSizePollerNoAlert(self):
        self.config.AlertGenerator.couchDbSizePoller.soft = 5
        self.config.AlertGenerator.couchDbSizePoller.critical = 10
        self.config.AlertGenerator.couchDbSizePoller.pollInterval = 0.2
        ti = utils.TestInput() # see attributes comments at the class
        ti.pollerClass = CouchDbSizePoller
        ti.config = self.config.AlertGenerator.couchDbSizePoller
        ti.thresholdToTest = self.config.AlertGenerator.couchDbSizePoller.soft - 3
        ti.expected = 0
        ti.thresholdDiff = 2
        ti.testCase = self
        utils.doDirectorySizePolling(ti)

        
    def testCouchPollerBasic(self):
        self.config.AlertGenerator.section_("bogusCouchPoller")
        self.config.AlertGenerator.bogusCouchPoller.soft = 1000
        self.config.AlertGenerator.bogusCouchPoller.critical = 2000
        self.config.AlertGenerator.bogusCouchPoller.pollInterval = 0.2
        self.config.AlertGenerator.bogusCouchPoller.period = 1
        try:        
            poller = CouchPoller(self.config.AlertGenerator.bogusCouchPoller,
                                 self.generator)
        except Exception, ex:
            self.fail("%s: exception: %s" % (self.testName, ex))
        # this class would not have defined polling sample function, give it one
        poller.sample = lambda proc: float(12)        
        poller.check()
        # assuming CouchDb server is running, check that 1 sensible measurement value was collected
        self.assertEqual(len(poller._measurements), 1)
        self.assertTrue(isinstance(poller._measurements[0], types.FloatType))
        
        
    def testCouchCPUPollerSoftThreshold(self):
        self.config.AlertGenerator.couchCPUPoller.soft = 70
        self.config.AlertGenerator.couchCPUPoller.critical = 80
        self.config.AlertGenerator.couchCPUPoller.pollInterval = 0.2
        self.config.AlertGenerator.couchCPUPoller.period = 1
        ppti = utils.TestInput() # see attributes comments at the class
        ppti.pollerClass = CouchCPUPoller       
        ppti.config = self.config.AlertGenerator.couchCPUPoller
        ppti.thresholdToTest = self.config.AlertGenerator.couchCPUPoller.soft
        ppti.level = self.config.AlertProcessor.all.level
        ppti.expected = 1
        ppti.thresholdDiff = 10
        ppti.testCase = self
        utils.doProcessPolling(ppti)
        

    def testCouchCPUPollerCriticalThreshold(self):
        self.config.AlertGenerator.couchCPUPoller.soft = 70
        self.config.AlertGenerator.couchCPUPoller.critical = 80
        self.config.AlertGenerator.couchCPUPoller.pollInterval = 0.2
        self.config.AlertGenerator.couchCPUPoller.period = 1
        ppti = utils.TestInput() # see attributes comments at the class
        ppti.pollerClass = CouchCPUPoller       
        ppti.config = self.config.AlertGenerator.couchCPUPoller
        ppti.thresholdToTest = self.config.AlertGenerator.couchCPUPoller.critical 
        ppti.level = self.config.AlertProcessor.critical.level
        ppti.expected = 1
        ppti.thresholdDiff = 10
        ppti.testCase = self
        utils.doProcessPolling(ppti)        

            
    def testCouchCPUPollerNoAlert(self):
        self.config.AlertGenerator.couchCPUPoller.soft = 70
        self.config.AlertGenerator.couchCPUPoller.critical = 80
        self.config.AlertGenerator.couchCPUPoller.pollInterval = 0.2
        self.config.AlertGenerator.couchCPUPoller.period = 1
        ppti = utils.TestInput() # see attributes comments at the class
        ppti.pollerClass = CouchCPUPoller       
        ppti.config = self.config.AlertGenerator.couchCPUPoller
        # lower the threshold so that the alert is never generated
        ppti.thresholdToTest = self.config.AlertGenerator.couchCPUPoller.soft - 20
        ppti.level = 0
        ppti.expected = 0
        ppti.thresholdDiff = 10
        ppti.testCase = self
        utils.doProcessPolling(ppti)
        

    def testCouchMemoryPollerSoftThreshold(self):
        self.config.AlertGenerator.couchMemPoller.soft = 70
        self.config.AlertGenerator.couchMemPoller.critical = 80
        self.config.AlertGenerator.couchMemPoller.pollInterval = 0.2
        self.config.AlertGenerator.couchMemPoller.period = 1
        ppti = utils.TestInput() # see attributes comments at the class
        ppti.pollerClass = CouchMemoryPoller
        ppti.config = self.config.AlertGenerator.couchMemPoller
        ppti.thresholdToTest = self.config.AlertGenerator.couchMemPoller.soft
        ppti.level = self.config.AlertProcessor.all.level
        ppti.expected = 1
        ppti.thresholdDiff = 10
        ppti.testCase = self
        utils.doProcessPolling(ppti)
                                    

    def testCouchMemoryPollerCriticalThreshold(self):
        self.config.AlertGenerator.couchMemPoller.soft = 70
        self.config.AlertGenerator.couchMemPoller.critical = 80
        self.config.AlertGenerator.couchMemPoller.pollInterval = 0.2
        self.config.AlertGenerator.couchMemPoller.period = 1
        ppti = utils.TestInput() # see attributes comments at the class
        ppti.pollerClass = CouchMemoryPoller
        ppti.config = self.config.AlertGenerator.couchMemPoller
        ppti.thresholdToTest = self.config.AlertGenerator.couchMemPoller.critical
        ppti.level = self.config.AlertProcessor.critical.level
        ppti.expected = 1
        ppti.thresholdDiff = 10
        ppti.testCase = self
        utils.doProcessPolling(ppti)
        

    def testCouchMemoryPollerNoAlert(self):
        self.config.AlertGenerator.couchMemPoller.soft = 70
        self.config.AlertGenerator.couchMemPoller.critical = 80
        self.config.AlertGenerator.couchMemPoller.pollInterval = 0.2
        self.config.AlertGenerator.couchMemPoller.period = 1
        ppti = utils.TestInput() # see attributes comments at the class
        ppti.pollerClass = CouchMemoryPoller
        ppti.config = self.config.AlertGenerator.couchMemPoller
        # lower the threshold so that the alert is never generated
        ppti.thresholdToTest = self.config.AlertGenerator.couchMemPoller.soft - 20
        ppti.level = 0
        ppti.expected = 0
        ppti.thresholdDiff = 10
        ppti.testCase = self
        utils.doProcessPolling(ppti)
        
        

if __name__ == "__main__":
    unittest.main()