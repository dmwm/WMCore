#!/usr/bin/env python

"""
WorkQueuManager test
"""

from builtins import range
from builtins import object
import threading
import unittest
from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit
from WMQuality.Emulators.AnalyticsDataCollector.DataCollectorAPI import REQUEST_NAME_PREFIX, NUM_REQUESTS

from WMComponent.AnalyticsDataCollector.AnalyticsPoller import AnalyticsPoller
from WMComponent.AnalyticsDataCollector.DataCollectorEmulatorSwitch import EmulatorHelper

class MockLocalQService(object):

    def __init__(self):
        pass

    def getAnalyticsData(self):
        """
        This getInject status and input dataset from workqueue
        """
        status = {}
        inputDataset = {}
        for i in range(NUM_REQUESTS + 1):
            status['%s%s' % (REQUEST_NAME_PREFIX, i + 1)] = {'inQueue': 1, 'inWMBS': 1}
            inputDataset['%s%s' % (REQUEST_NAME_PREFIX, i + 1)] = 'inputdataset-%s' % (i + 1)

        return {'status': status, 'input_dataset': inputDataset}

class AnalyticsDataCollector_t(unittest.TestCase):
    """
    TestCase for WorkQueueManagerTest module
    """

    def setUp(self):
        """
        setup for test.
        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules=["WMCore.WMBS"],
                                useDefault=False)
        self.testInit.setSchema(customModules=["WMComponent.DBS3Buffer"],
                                useDefault=False)
        self.reqmonDBName = "wmstat_t"
        self.localDBName = "wmstat_t_local"
        self.testInit.setupCouch(self.reqmonDBName, "WMStats")
        self.testInit.setupCouch(self.localDBName, "WMStats")
        self.testDir = self.testInit.generateWorkDir()
        EmulatorHelper.setEmulators(localCouch=True, reqMon=False, wmagentDB=True)
        return

    def tearDown(self):
        """
        Database deletion
        """
        threading.currentThread()

        self.testInit.delWorkDir()
        self.testInit.tearDownCouch()
        self.testInit.clearDatabase()
        EmulatorHelper.resetEmulators()
        return

    def getConfig(self):
        """
        _createConfig_

        General config file
        """
        couchURL = self.testInit.couchUrl
        config = self.testInit.getConfiguration()
        self.testInit.generateWorkDir(config)
        config.section_("Agent")
        config.Agent.hostName = "localhost"
        config.Agent.contact = "test@test.com"
        config.Agent.teamName = "testTeam"
        config.Agent.agentName = "testAgentName"
        config.Agent.useMsgService = False
        config.Agent.useTrigger = False
        config.Agent.useHeartbeat = False

        config.section_("General")
        config.General.centralWMStatsURL = "%s/%s" % (couchURL, self.reqmonDBName)

        config.section_("ACDC")
        config.ACDC.couchurl = couchURL
        config.ACDC.database = "acdc"

        config.component_("AnalyticsDataCollector")
        config.AnalyticsDataCollector.namespace = "WMComponent.AnalyticsDataCollector.AnalyticsDataCollector"
        config.AnalyticsDataCollector.componentDir = config.General.workDir + "/AnalyticsDataCollector"
        config.AnalyticsDataCollector.logLevel = "DEBUG"
        config.AnalyticsDataCollector.pollInterval = 240
        config.AnalyticsDataCollector.localCouchURL = "%s/%s" % (couchURL, "jobDump")
        config.AnalyticsDataCollector.localQueueURL = "%s/%s" % (couchURL, "workqueue")
        config.AnalyticsDataCollector.localWMStatsURL = "%s/%s" % (couchURL, self.localDBName)
        config.AnalyticsDataCollector.centralRequestDBURL = "%s/%s" % (couchURL, "requset_db_t")
        config.AnalyticsDataCollector.reqMonURL = "%s/%s" % (couchURL, self.reqmonDBName)
        config.AnalyticsDataCollector.RequestCouchApp = "ReqMgr"
        config.AnalyticsDataCollector.summaryLevel = "task"

        return config

    def testAnalyticsPoller(self):
        """
        Tests the components, as in sees if they load.
        Otherwise does nothing.
        """
        threading.currentThread()
        config = self.getConfig()
        analytics = AnalyticsPoller(config)

        analytics.setup(None)
        analytics.localQueue = MockLocalQService()
        analytics.algorithm(None)
        return

if __name__ == '__main__':
    unittest.main()
