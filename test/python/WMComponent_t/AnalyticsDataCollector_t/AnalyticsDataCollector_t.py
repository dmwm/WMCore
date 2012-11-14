#!/usr/bin/env python

"""
WorkQueuManager test
"""

import os
import logging
import threading
import unittest
import time
from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit
from WMQuality.Emulators.AnalyticsDataCollector.DataCollectorAPI import REQUEST_NAME_PREFIX, NUM_REQUESTS

from WMCore.Agent.Configuration import loadConfigurationFile
from WMCore.DAOFactory import DAOFactory
from WMComponent.AnalyticsDataCollector.AnalyticsDataCollector import AnalyticsDataCollector
from WMComponent.AnalyticsDataCollector.AnalyticsPoller import AnalyticsPoller
from WMComponent.AnalyticsDataCollector.DataCollectorEmulatorSwitch import EmulatorHelper

class MockLocalQService():

    def __init__(self):
        pass

    def getAnalyticsData(self):
        """
        This getInject status and input dataset from workqueue
        """
        status = {}
        inputDataset = {}
        for i in range(NUM_REQUESTS + 1):
            status['%s%s' % (REQUEST_NAME_PREFIX, i+1)] = {'inQueue': 1, 'inWMBS': 1}
            inputDataset['%s%s' % (REQUEST_NAME_PREFIX, i+1)] = 'inputdataset-%s' % (i+1)

        return {'status': status, 'input_dataset': inputDataset}

class AnalyticsDataCollector_t(unittest.TestCase):
    """
    TestCase for WorkQueueManagerTest module
    """

    def setUp(self):
        """
        setup for test.
        """

        myThread = threading.currentThread()
        myThread.dbFactory = None
        myThread.logger = None
        myThread.dbi = None
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.reqmonDBName = "wmstat_t"
        self.localDBName = "wmstat_t_local"
        self.testInit.setupCouch(self.reqmonDBName, "WMStats")
        self.testInit.setupCouch(self.localDBName, "WMStats")
        self.testDir = self.testInit.generateWorkDir()
        EmulatorHelper.setEmulators(localCouch = True, reqMon = False, wmagentDB = True)
        return

    def tearDown(self):
        """
        Database deletion
        """
        myThread = threading.currentThread()

        self.testInit.delWorkDir()
        self.testInit.tearDownCouch()
        EmulatorHelper.resetEmulators()
        return

    def getConfig(self):
        """
        _createConfig_

        General config file
        """
        #configPath=os.path.join(WMCore.WMInit.getWMBASE(), \
        #                        'src/python/WMComponent/WorkQueueManager/DefaultConfig.py')):

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
        config.AnalyticsDataCollector.centralWMStatsURL = "%s/%s" % (couchURL, self.reqmonDBName)
        config.AnalyticsDataCollector.reqMonURL = "%s/%s" % (couchURL, self.reqmonDBName)
        config.AnalyticsDataCollector.summaryLevel = "task"

        config.section_("WMBSService")
        config.WMBSService.section_("Webtools")
        config.WMBSService.Webtools.port = 9999

        return config

    def testAnalyticsPoller(self):
        """
        Tests the components, as in sees if they load.
        Otherwise does nothing.
        """
        myThread = threading.currentThread()
        config = self.getConfig()
        analytics = AnalyticsPoller(config)

        analytics.setup(None)
        analytics.localQueue = MockLocalQService()
        analytics.algorithm(None)
        return

if __name__ == '__main__':
    unittest.main()
