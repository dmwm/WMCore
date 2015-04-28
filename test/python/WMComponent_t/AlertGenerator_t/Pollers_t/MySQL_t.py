"""
Tests for all MySQL related metrics pollers.

"""


import os
import unittest
import logging
import types
import random
import time

from WMQuality.TestInit import TestInit
from WMComponent.AlertGenerator.Pollers.MySQL import MySQLPoller
from WMComponent.AlertGenerator.Pollers.MySQL import MySQLDbSizePoller
from WMComponent.AlertGenerator.Pollers.MySQL import MySQLCPUPoller
from WMComponent.AlertGenerator.Pollers.MySQL import MySQLMemoryPoller
from WMComponent.AlertGenerator.Pollers.Base import ProcessDetail
from WMComponent.AlertGenerator.Pollers.Base import Measurements
from WMComponent_t.AlertGenerator_t.AlertGenerator_t import getConfig
from WMComponent_t.AlertGenerator_t.Pollers_t import utils



class MySQLTest(unittest.TestCase):
    def setUp(self):
        self.testInit = TestInit(__file__)
        self.testInit.setLogging(logLevel = logging.DEBUG)
        self.testInit.setDatabaseConnection()
        self.testDir = self.testInit.generateWorkDir()
        self.config = getConfig(self.testDir)
        # mock generator instance to communicate some configuration values
        self.generator = utils.AlertGeneratorMock(self.config)
        self.testName = self.id().split('.')[-1]


    def tearDown(self):
        self.testInit.delWorkDir()
        self.generator = None


    def testMySQLPollerBasic(self):
        config = getConfig("/tmp")
        generator = utils.AlertGeneratorMock(config)
        # take for instance mysqlCPUPoller configuration here, just need
        # appropriate attributes set
        try:
            poller = MySQLPoller(config.AlertGenerator.mysqlCPUPoller, generator)
        except Exception as ex:
            self.fail("%s: exception: %s" % (self.testName, ex))
        # this class would not have defined polling sample function, give it one
        poller.sample = lambda proc: float(12)
        self.assertEqual(len(poller._measurements), 0)
        poller.check()
        self.assertEqual(len(poller._measurements), 1)
        self.assertEqual(poller._measurements[0], 12)

        # test handling of a non-existing process
        MySQLPoller._getProcessPID = lambda inst: 1212121212
        self.assertRaises(Exception, MySQLPoller,
                          config.AlertGenerator.mysqlCPUPoller, generator)


    def testMySQLCPUPollerBasic(self):
        config = getConfig("/tmp")
        generator = utils.AlertGeneratorMock(config)
        try:
            poller = MySQLCPUPoller(config.AlertGenerator.mysqlCPUPoller, generator)
        except Exception as ex:
            self.fail("%s: exception: %s" % (self.testName, ex))
        self.assertEqual(len(poller._measurements), 0)
        poller.check()
        # assuming MySQL server is running, check that 1 sensible measurement value was collected
        self.assertEqual(len(poller._measurements), 1)
        self.assertTrue(isinstance(poller._measurements[0], types.FloatType))


    def testMySQLCPUPollerSoftThreshold(self):
        self.config.AlertGenerator.mysqlCPUPoller.soft = 70
        self.config.AlertGenerator.mysqlCPUPoller.critical = 80
        self.config.AlertGenerator.mysqlCPUPoller.pollInterval = 0.2
        self.config.AlertGenerator.mysqlCPUPoller.period = 1
        ti = utils.TestInput() # see attributes comments at the class
        ti.pollerClass = MySQLCPUPoller
        ti.config = self.config.AlertGenerator.mysqlCPUPoller
        ti.thresholdToTest = self.config.AlertGenerator.mysqlCPUPoller.soft
        ti.level = self.config.AlertProcessor.soft.level
        ti.expected = 1
        ti.thresholdDiff = 10
        ti.testCase = self
        utils.doGenericPeriodAndProcessPolling(ti)


    def testMySQLCPUPollerCriticalThreshold(self):
        self.config.AlertGenerator.mysqlCPUPoller.soft = 70
        self.config.AlertGenerator.mysqlCPUPoller.critical = 80
        self.config.AlertGenerator.mysqlCPUPoller.pollInterval = 0.2
        self.config.AlertGenerator.mysqlCPUPoller.period = 1
        ti = utils.TestInput() # see attributes comments at the class
        ti.pollerClass = MySQLCPUPoller
        ti.config = self.config.AlertGenerator.mysqlCPUPoller
        ti.thresholdToTest = self.config.AlertGenerator.mysqlCPUPoller.critical
        ti.level = self.config.AlertProcessor.critical.level
        ti.expected = 1
        ti.thresholdDiff = 10
        ti.testCase = self
        utils.doGenericPeriodAndProcessPolling(ti)


    def testMySQLCPUPollerNoAlert(self):
        self.config.AlertGenerator.mysqlCPUPoller.soft = 70
        self.config.AlertGenerator.mysqlCPUPoller.critical = 80
        self.config.AlertGenerator.mysqlCPUPoller.pollInterval = 0.2
        self.config.AlertGenerator.mysqlCPUPoller.period = 1
        ti = utils.TestInput() # see attributes comments at the class
        ti.pollerClass = MySQLCPUPoller
        ti.config = self.config.AlertGenerator.mysqlCPUPoller
        # lower the threshold so that the alert is never generated
        ti.thresholdToTest = self.config.AlertGenerator.mysqlCPUPoller.soft - 20
        ti.level = 0
        ti.expected = 0
        ti.thresholdDiff = 10
        ti.testCase = self
        utils.doGenericPeriodAndProcessPolling(ti)


    def testMySQLMemoryPollerSoftThreshold(self):
        self.config.AlertGenerator.mysqlMemPoller.soft = 70
        self.config.AlertGenerator.mysqlMemPoller.critical = 80
        self.config.AlertGenerator.mysqlMemPoller.pollInterval = 0.2
        self.config.AlertGenerator.mysqlMemPoller.period = 1
        ti = utils.TestInput() # see attributes comments at the class
        ti.pollerClass = MySQLMemoryPoller
        ti.config = self.config.AlertGenerator.mysqlMemPoller
        ti.thresholdToTest = self.config.AlertGenerator.mysqlMemPoller.soft
        ti.level = self.config.AlertProcessor.soft.level
        ti.expected = 1
        ti.thresholdDiff = 10
        ti.testCase = self
        utils.doGenericPeriodAndProcessPolling(ti)


    def testMySQLMemoryPollerCriticalThreshold(self):
        self.config.AlertGenerator.mysqlMemPoller.soft = 70
        self.config.AlertGenerator.mysqlMemPoller.critical = 80
        self.config.AlertGenerator.mysqlMemPoller.pollInterval = 0.2
        self.config.AlertGenerator.mysqlMemPoller.period = 1
        ti = utils.TestInput() # see attributes comments at the class
        ti.pollerClass = MySQLMemoryPoller
        ti.config = self.config.AlertGenerator.mysqlMemPoller
        ti.thresholdToTest = self.config.AlertGenerator.mysqlMemPoller.critical
        ti.level = self.config.AlertProcessor.critical.level
        ti.expected = 1
        ti.thresholdDiff = 10
        ti.testCase = self
        utils.doGenericPeriodAndProcessPolling(ti)


    def testMySQLMemoryPollerNoAlert(self):
        self.config.AlertGenerator.mysqlMemPoller.soft = 70
        self.config.AlertGenerator.mysqlMemPoller.critical = 80
        self.config.AlertGenerator.mysqlMemPoller.pollInterval = 0.2
        self.config.AlertGenerator.mysqlMemPoller.period = 1
        ti = utils.TestInput() # see attributes comments at the class
        ti.pollerClass = MySQLMemoryPoller
        ti.config = self.config.AlertGenerator.mysqlMemPoller
        # lower the threshold so that the alert is never generated
        ti.thresholdToTest = self.config.AlertGenerator.mysqlMemPoller.soft - 20
        ti.level = 0
        ti.expected = 0
        ti.thresholdDiff = 10
        ti.testCase = self
        utils.doGenericPeriodAndProcessPolling(ti)


    def testMySQLDbSizePollerBasic(self):
        config = getConfig("/tmp")
        generator = utils.AlertGeneratorMock(config)
        try:
            poller = MySQLDbSizePoller(config.AlertGenerator.mysqlCPUPoller, generator)
        except Exception as ex:
            self.fail("%s: exception: %s" % (self.testName, ex))
        poller.check()

        # test failing during set up
        poller = MySQLDbSizePoller(config.AlertGenerator.mysqlCPUPoller, generator)
        poller._query = "nonsense query"
        # this will fail on the above query
        self.assertRaises(Exception, poller._getDbDir)
        poller.check()


    def testAlertGeneratorMySQLDbSizePollerSoftThreshold(self):
        self.config.AlertGenerator.mysqlDbSizePoller.soft = 5
        self.config.AlertGenerator.mysqlDbSizePoller.critical = 10
        self.config.AlertGenerator.mysqlDbSizePoller.pollInterval = 0.2
        ti = utils.TestInput() # see attributes comments at the class
        ti.pollerClass = MySQLDbSizePoller
        ti.config = self.config.AlertGenerator.mysqlDbSizePoller
        ti.thresholdToTest = self.config.AlertGenerator.mysqlDbSizePoller.soft
        ti.level = self.config.AlertProcessor.soft.level
        ti.expected = 1
        ti.thresholdDiff = 1
        ti.testCase = self
        utils.doGenericValueBasedPolling(ti)


    def testAlertGeneratorMySQLDbSizePollerCriticalThreshold(self):
        self.config.AlertGenerator.mysqlDbSizePoller.soft = 5
        self.config.AlertGenerator.mysqlDbSizePoller.critical = 10
        self.config.AlertGenerator.mysqlDbSizePoller.pollInterval = 0.2
        ti = utils.TestInput() # see attributes comments at the class
        ti.pollerClass = MySQLDbSizePoller
        ti.config = self.config.AlertGenerator.mysqlDbSizePoller
        ti.thresholdToTest = self.config.AlertGenerator.mysqlDbSizePoller.critical
        ti.level = self.config.AlertProcessor.critical.level
        ti.expected = 1
        ti.thresholdDiff = 1
        ti.testCase = self
        utils.doGenericValueBasedPolling(ti)


    def testAlertGeneratorMySQLDbSizePollerNoAlert(self):
        self.config.AlertGenerator.mysqlDbSizePoller.soft = 5
        self.config.AlertGenerator.mysqlDbSizePoller.critical = 10
        self.config.AlertGenerator.mysqlDbSizePoller.pollInterval = 0.2
        ti = utils.TestInput() # see attributes comments at the class
        ti.pollerClass = MySQLDbSizePoller
        ti.config = self.config.AlertGenerator.mysqlDbSizePoller
        # lower the threshold so that the alert is never generated
        ti.thresholdToTest = self.config.AlertGenerator.mysqlDbSizePoller.soft - 3
        ti.level = 0
        ti.expected = 0
        ti.thresholdDiff = 2
        ti.testCase = self
        utils.doGenericValueBasedPolling(ti)



if __name__ == "__main__":
    unittest.main()
