"""
Tests for all CouchDb related metrics pollers for the Alerts messaging
framework.

"""

import os
import unittest
import logging
import types
import random
import time

from WMQuality.TestInit import TestInit
from WMComponent_t.AlertGenerator_t.AlertGenerator_t import getConfig
from WMComponent_t.AlertGenerator_t.Pollers_t import utils
from WMComponent.AlertGenerator.Pollers.Couch import CouchPoller
from WMComponent.AlertGenerator.Pollers.Couch import CouchDbSizePoller
from WMComponent.AlertGenerator.Pollers.Couch import CouchCPUPoller
from WMComponent.AlertGenerator.Pollers.Couch import CouchMemoryPoller
from WMComponent.AlertGenerator.Pollers.Couch import CouchErrorsPoller



class CouchTest(unittest.TestCase):
    def setUp(self):
        self.testInit = TestInit(__file__)
        self.testInit.setLogging(logLevel = logging.DEBUG)
        self.testDir = self.testInit.generateWorkDir()
        self.config = getConfig(self.testDir)
        # mock generator instance to communicate some configuration values
        self.generator = utils.AlertGeneratorMock(self.config)
        self.testName = self.id().split('.')[-1]


    def tearDown(self):
        self.testInit.delWorkDir()
        self.generator = None


    def testAlertGeneratorCouchDbSizePollerBasic(self):
        config = getConfig("/tmp")
        try:
            poller = CouchDbSizePoller(config.AlertGenerator.couchDbSizePoller, self.generator)
        except Exception as ex:
            self.fail("%s: exception: %s" % (self.testName, ex))
        poller.check() # -> on real system dir may result in permission denied
        poller._dbDirectory = "/dev"
        poller.check() # -> OK

        # test failing during set up
        poller = CouchDbSizePoller(config.AlertGenerator.couchDbSizePoller, self.generator)
        poller._query = "nonsense query"
        # this will fail on the above query
        self.assertRaises(Exception, poller._getDbDir)
        poller.check()


    def testAlertGeneratorCouchDbSizePollerSoftThreshold(self):
        self.config.AlertGenerator.couchDbSizePoller.couchURL = os.getenv("COUCHURL", None)
        self.config.AlertGenerator.couchDbSizePoller.soft = 5
        self.config.AlertGenerator.couchDbSizePoller.critical = 10
        self.config.AlertGenerator.couchDbSizePoller.pollInterval = 0.2
        ti = utils.TestInput() # see attributes comments at the class
        ti.pollerClass = CouchDbSizePoller
        ti.config = self.config.AlertGenerator.couchDbSizePoller
        ti.thresholdToTest = self.config.AlertGenerator.couchDbSizePoller.soft
        ti.level = self.config.AlertProcessor.soft.level
        ti.expected = 1
        ti.thresholdDiff = 4
        ti.testCase = self
        utils.doGenericValueBasedPolling(ti)


    def testAlertGeneratorCouchDbSizePollerCriticalThreshold(self):
        self.config.AlertGenerator.couchDbSizePoller.couchURL = os.getenv("COUCHURL", None)
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
        utils.doGenericValueBasedPolling(ti)


    def testAlertGeneratorCouchDbSizePollerNoAlert(self):
        self.config.AlertGenerator.couchDbSizePoller.couchURL = os.getenv("COUCHURL", None)
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
        utils.doGenericValueBasedPolling(ti)


    def testAlertGeneratorCouchPollerBasic(self):
        self.config.AlertGenerator.section_("bogusCouchPoller")
        self.config.AlertGenerator.bogusCouchPoller.couchURL = os.getenv("COUCHURL", None)
        self.config.AlertGenerator.bogusCouchPoller.soft = 1000
        self.config.AlertGenerator.bogusCouchPoller.critical = 2000
        self.config.AlertGenerator.bogusCouchPoller.pollInterval = 0.2
        self.config.AlertGenerator.bogusCouchPoller.period = 1
        try:
            poller = CouchPoller(self.config.AlertGenerator.bogusCouchPoller,
                                 self.generator)
        except Exception as ex:
            self.fail("%s: exception: %s" % (self.testName, ex))
        # this class would not have defined polling sample function, give it one
        poller.sample = lambda proc: float(12)
        poller.check()
        # assuming CouchDb server is running, check that 1 sensible measurement value was collected
        self.assertEqual(len(poller._measurements), 1)
        self.assertTrue(isinstance(poller._measurements[0], float))
        # test handling of a non-existing process
        CouchPoller._getProcessPID = lambda inst: 1212121212
        self.assertRaises(Exception, CouchPoller,
                          self.config.AlertGenerator.bogusCouchPoller, self.generator)


    def testAlertGeneratorCouchCPUPollerSoftThreshold(self):
        self.config.AlertGenerator.couchCPUPoller.couchURL = os.getenv("COUCHURL", None)
        self.config.AlertGenerator.couchCPUPoller.soft = 70
        self.config.AlertGenerator.couchCPUPoller.critical = 80
        self.config.AlertGenerator.couchCPUPoller.pollInterval = 0.2
        self.config.AlertGenerator.couchCPUPoller.period = 1
        ti = utils.TestInput() # see attributes comments at the class
        ti.pollerClass = CouchCPUPoller
        ti.config = self.config.AlertGenerator.couchCPUPoller
        ti.thresholdToTest = self.config.AlertGenerator.couchCPUPoller.soft
        ti.level = self.config.AlertProcessor.soft.level
        ti.expected = 1
        ti.thresholdDiff = 10
        ti.testCase = self
        utils.doGenericPeriodAndProcessPolling(ti)


    def testAlertGeneratorCouchCPUPollerCriticalThreshold(self):
        self.config.AlertGenerator.couchCPUPoller.couchURL = os.getenv("COUCHURL", None)
        self.config.AlertGenerator.couchCPUPoller.soft = 70
        self.config.AlertGenerator.couchCPUPoller.critical = 80
        self.config.AlertGenerator.couchCPUPoller.pollInterval = 0.2
        self.config.AlertGenerator.couchCPUPoller.period = 1
        ti = utils.TestInput() # see attributes comments at the class
        ti.pollerClass = CouchCPUPoller
        ti.config = self.config.AlertGenerator.couchCPUPoller
        ti.thresholdToTest = self.config.AlertGenerator.couchCPUPoller.critical
        ti.level = self.config.AlertProcessor.critical.level
        ti.expected = 1
        ti.thresholdDiff = 10
        ti.testCase = self
        utils.doGenericPeriodAndProcessPolling(ti)


    def testAlertGeneratorCouchCPUPollerNoAlert(self):
        self.config.AlertGenerator.couchCPUPoller.couchURL = os.getenv("COUCHURL", None)
        self.config.AlertGenerator.couchCPUPoller.soft = 70
        self.config.AlertGenerator.couchCPUPoller.critical = 80
        self.config.AlertGenerator.couchCPUPoller.pollInterval = 0.2
        self.config.AlertGenerator.couchCPUPoller.period = 1
        ti = utils.TestInput() # see attributes comments at the class
        ti.pollerClass = CouchCPUPoller
        ti.config = self.config.AlertGenerator.couchCPUPoller
        # lower the threshold so that the alert is never generated
        ti.thresholdToTest = self.config.AlertGenerator.couchCPUPoller.soft - 20
        ti.level = 0
        ti.expected = 0
        ti.thresholdDiff = 10
        ti.testCase = self
        utils.doGenericPeriodAndProcessPolling(ti)


    def testAlertGeneratorCouchMemoryPollerSoftThreshold(self):
        self.config.AlertGenerator.couchMemPoller.couchURL = os.getenv("COUCHURL", None)
        self.config.AlertGenerator.couchMemPoller.soft = 70
        self.config.AlertGenerator.couchMemPoller.critical = 80
        self.config.AlertGenerator.couchMemPoller.pollInterval = 0.2
        self.config.AlertGenerator.couchMemPoller.period = 1
        ti = utils.TestInput() # see attributes comments at the class
        ti.pollerClass = CouchMemoryPoller
        ti.config = self.config.AlertGenerator.couchMemPoller
        ti.thresholdToTest = self.config.AlertGenerator.couchMemPoller.soft
        ti.level = self.config.AlertProcessor.soft.level
        ti.expected = 1
        ti.thresholdDiff = 10
        ti.testCase = self
        utils.doGenericPeriodAndProcessPolling(ti)


    def testAlertGeneratorCouchMemoryPollerCriticalThreshold(self):
        self.config.AlertGenerator.couchMemPoller.couchURL = os.getenv("COUCHURL", None)
        self.config.AlertGenerator.couchMemPoller.soft = 70
        self.config.AlertGenerator.couchMemPoller.critical = 80
        self.config.AlertGenerator.couchMemPoller.pollInterval = 0.2
        self.config.AlertGenerator.couchMemPoller.period = 1
        ti = utils.TestInput() # see attributes comments at the class
        ti.pollerClass = CouchMemoryPoller
        ti.config = self.config.AlertGenerator.couchMemPoller
        ti.thresholdToTest = self.config.AlertGenerator.couchMemPoller.critical
        ti.level = self.config.AlertProcessor.critical.level
        ti.expected = 1
        ti.thresholdDiff = 10
        ti.testCase = self
        utils.doGenericPeriodAndProcessPolling(ti)


    def testAlertGeneratorCouchMemoryPollerNoAlert(self):
        self.config.AlertGenerator.couchMemPoller.couchURL = os.getenv("COUCHURL", None)
        self.config.AlertGenerator.couchMemPoller.soft = 70
        self.config.AlertGenerator.couchMemPoller.critical = 80
        self.config.AlertGenerator.couchMemPoller.pollInterval = 0.2
        self.config.AlertGenerator.couchMemPoller.period = 1
        ti = utils.TestInput() # see attributes comments at the class
        ti.pollerClass = CouchMemoryPoller
        ti.config = self.config.AlertGenerator.couchMemPoller
        # lower the threshold so that the alert is never generated
        ti.thresholdToTest = self.config.AlertGenerator.couchMemPoller.soft - 20
        ti.level = 0
        ti.expected = 0
        ti.thresholdDiff = 10
        ti.testCase = self
        utils.doGenericPeriodAndProcessPolling(ti)


    def testAlertGeneratorCouchErrorsPollerBasic(self):
        try:
            poller = CouchErrorsPoller(self.config.AlertGenerator.couchErrorsPoller, self.generator)
        except Exception as ex:
            self.fail("Exception, reason: %s" % ex)

        # even a single values to observe shall be turned into particular iterables
        obs = self.config.AlertGenerator.couchErrorsPoller.observables
        self.config.AlertGenerator.couchErrorsPoller.observables = 400
        try:
            poller = CouchErrorsPoller(self.config.AlertGenerator.couchErrorsPoller, self.generator)
        except Exception as ex:
            self.fail("Exception, reason: %s" % ex)
        #self.assertTrue(isinstance(obs, (types.ListType, types.TupleType)))
        self.assertTrue(isinstance(self.config.AlertGenerator.couchErrorsPoller.observables,
                                   (list, tuple)))

        # test return value on non-sense HTTP status code
        res = poller.sample("40000")
        self.assertFalse(res)
        # test definitely existing value
        res = poller.sample("200")
        # on a freshly started couch, this status code may not exist in the
        # stats table, so despite correct and meaningful HTTP status code, the
        # query may still return None, hence don't assume any particular response
        #self.assertTrue(isinstance(res, types.IntType))
        poller.check()


    def testAlertGeneratorCouchErrorsPollerSoftThreshold(self):
        self.config.AlertGenerator.couchErrorsPoller.couchURL = os.getenv("COUCHURL", None)
        self.config.AlertGenerator.couchErrorsPoller.soft = 100
        self.config.AlertGenerator.couchErrorsPoller.critical = 200
        # shall expect corresponding number of generated alerts for each observable value
        self.config.AlertGenerator.couchErrorsPoller.observables = (5, 6, 7)
        self.config.AlertGenerator.couchErrorsPoller.pollInterval = 0.2
        ti = utils.TestInput() # see attributes comments at the class
        ti.pollerClass = CouchErrorsPoller
        ti.config = self.config.AlertGenerator.couchErrorsPoller
        ti.thresholdToTest = self.config.AlertGenerator.couchErrorsPoller.soft
        ti.level = self.config.AlertProcessor.soft.level
        ti.expected = len(self.config.AlertGenerator.couchErrorsPoller.observables)
        ti.thresholdDiff = 10
        ti.testCase = self
        utils.doGenericValueBasedPolling(ti)


    def testAlertGeneratorCouchErrorsPollerCriticalThreshold(self):
        self.config.AlertGenerator.couchErrorsPoller.couchURL = os.getenv("COUCHURL", None)
        self.config.AlertGenerator.couchErrorsPoller.soft = 100
        self.config.AlertGenerator.couchErrorsPoller.critical = 200
        # shall expect corresponding number of generated alerts for each observable value
        self.config.AlertGenerator.couchErrorsPoller.observables = (5, 6, 7)
        self.config.AlertGenerator.couchErrorsPoller.pollInterval = 0.2
        ti = utils.TestInput() # see attributes comments at the class
        ti.pollerClass = CouchErrorsPoller
        ti.config = self.config.AlertGenerator.couchErrorsPoller
        ti.thresholdToTest = self.config.AlertGenerator.couchErrorsPoller.critical
        ti.level = self.config.AlertProcessor.critical.level
        ti.expected = len(self.config.AlertGenerator.couchErrorsPoller.observables)
        ti.thresholdDiff = 50
        ti.testCase = self
        utils.doGenericValueBasedPolling(ti)


    def testAlertGeneratorCouchErrorsPollerNoAlert(self):
        self.config.AlertGenerator.couchErrorsPoller.couchURL = os.getenv("COUCHURL", None)
        self.config.AlertGenerator.couchErrorsPoller.soft = 100
        self.config.AlertGenerator.couchErrorsPoller.critical = 200
        # shall expect corresponding number of generated alerts for each observable value
        self.config.AlertGenerator.couchErrorsPoller.observables = (5, 6, 7)
        self.config.AlertGenerator.couchErrorsPoller.pollInterval = 0.2
        ti = utils.TestInput() # see attributes comments at the class
        ti.pollerClass = CouchErrorsPoller
        ti.config = self.config.AlertGenerator.couchErrorsPoller
        ti.thresholdToTest = self.config.AlertGenerator.couchErrorsPoller.soft - 30
        ti.expected = 0
        ti.thresholdDiff = 20
        ti.testCase = self
        utils.doGenericValueBasedPolling(ti)



if __name__ == "__main__":
    unittest.main()
