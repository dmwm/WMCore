#!/usr/bin/env python
"""
AgentStatusWatcher unittest
"""

from __future__ import print_function, division

import unittest

import logging
import threading

from WMComponent.AgentStatusWatcher.ResourceControlUpdater import ResourceControlUpdater
from WMComponent.AgentStatusWatcher.AgentStatusPoller import AgentStatusPoller
from WMComponent.AgentStatusWatcher.DrainStatusPoller import DrainStatusPoller
from WMCore.Agent.Configuration import Configuration
from WMComponent.AnalyticsDataCollector.DataCollectorEmulatorSwitch import EmulatorHelper
from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit

class AgentStatusWatcherTest(unittest.TestCase):
    """
    TestCase for AgentStatusWatcher threads
    """
    def setUp(self):
        """
        setup for test.
        """
        myThread = threading.currentThread()
        myThread.dbFactory = None
        myThread.dbi = None
        myThread.logger = logging

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.reqmonDBName = "wmstat_t"
        self.localDBName = "wmstat_t_local"
        self.reqDBName = "reqconfig_t"
        self.testInit.setupCouch(self.reqmonDBName, "WMStats")
        self.testInit.setupCouch(self.localDBName, "WMStats")
        self.testDir = self.testInit.generateWorkDir()
        EmulatorHelper.setEmulators(localCouch=True, reqMon=False, wmagentDB=True)
        return

    def tearDown(self):

        self.testInit.delWorkDir()
        self.testInit.tearDownCouch()
        EmulatorHelper.resetEmulators()
        return

    def getConfig(self):

        couchURL = self.testInit.couchUrl

        config = Configuration()
        config.section_("Agent")
        config.Agent.hostName = "localhost"
        config.Agent.contact = "test@test.com"
        config.Agent.teamName = "testTeam"
        config.Agent.agentName = "testAgentName"
        config.Agent.useMsgService = False
        config.Agent.useTrigger = False
        config.Agent.useHeartbeat = False

        config.section_("General")
        config.General.workDir = "/tmp"

        config.component_("AgentStatusWatcher")
        config.AgentStatusWatcher.namespace = "WMComponent.AgentStatusWatcher.AgentStatusWatcher"
        config.AgentStatusWatcher.componentDir = config.General.workDir
        config.AgentStatusWatcher.logLevel = "INFO"
        config.AgentStatusWatcher.resourceUpdaterPollInterval = 900
        config.AgentStatusWatcher.siteStatusMetric = 237
        config.AgentStatusWatcher.cpuBoundMetric = 160
        config.AgentStatusWatcher.ioBoundMetric = 161
        config.AgentStatusWatcher.dashboard = "Dashboard URL"
        config.AgentStatusWatcher.centralWMStatsURL = "%s/%s" % (couchURL, self.reqmonDBName)
        config.AgentStatusWatcher.pendingSlotsSitePercent = 100
        config.AgentStatusWatcher.pendingSlotsTaskPercent = 90
        config.AgentStatusWatcher.runningExpressPercent = 30
        config.AgentStatusWatcher.runningRepackPercent = 10
        config.AgentStatusWatcher.t1SitesCores = 30
        config.AgentStatusWatcher.forceSiteDown = []
        config.AgentStatusWatcher.onlySSB = False
        config.AgentStatusWatcher.enabled = True
        config.AgentStatusWatcher.agentPollInterval = 300
        config.AgentStatusWatcher.drainStatusPollInterval = 1800
        config.AgentStatusWatcher.defaultAgentsNumByTeam = 5
        config.AgentStatusWatcher.jsonFile = config.AgentStatusWatcher.componentDir + "/WMA_monitoring.json"

        config.section_("ACDC")
        config.ACDC.couchurl = couchURL
        config.ACDC.database = "acdc"

        config.component_("AnalyticsDataCollector")
        config.AnalyticsDataCollector.summaryLevel = "task"
        config.AnalyticsDataCollector.centralWMStatsURL = "%s/%s" % (couchURL, self.reqmonDBName)
        config.AnalyticsDataCollector.localQueueURL = "%s/%s" % (couchURL, "workqueue")
        config.AnalyticsDataCollector.diskUseThreshold = 75
        config.AnalyticsDataCollector.ignoreDisk = []
        config.AnalyticsDataCollector.couchProcessThreshold = 50

        config.section_("JobStateMachine")
        config.JobStateMachine.couchurl = couchURL
        config.JobStateMachine.couchDBName = "wmagent_jobdump"

        # set a different location for couchdb replication
        config.JobStateMachine.jobSummaryDBName = "%s/%s" % (couchURL, "wmagent_summary_rep")
        config.JobStateMachine.summaryStatsDBName = "stat_summary"

        config.component_('WorkQueueManager')
        config.WorkQueueManager.namespace = "WMComponent.WorkQueueManager.WorkQueueManager"
        config.WorkQueueManager.componentDir = config.General.workDir + "/WorkQueueManager"
        config.WorkQueueManager.level = 'LocalQueue'
        config.WorkQueueManager.logLevel = 'INFO'
        config.WorkQueueManager.couchurl = couchURL
        config.WorkQueueManager.dbname = 'workqueue'
        config.WorkQueueManager.inboxDatabase = 'workqueue_inbox'
        config.WorkQueueManager.queueParams = {}
        config.WorkQueueManager.queueParams["ParentQueueCouchUrl"] = "https://localhost/couchdb/workqueue"
        config.WorkQueueManager.queueParams["QueueURL"] = "http://%s:5984" % (config.Agent.hostName)
        config.WorkQueueManager.queueParams["WorkPerCycle"] = 100

        return config

    def testAgentStatusPoller(self):
        # test agent status thread
        config = self.getConfig()
        agentStats = AgentStatusPoller(config)
        agentStats.setup(None)
        agentStats.algorithm(None)
        return

    def testDrainStatusPoller(self):
        # test drain status thread
        config = self.getConfig()
        drainStats = DrainStatusPoller(config)
        drainStats.setup(None)
        drainStats.algorithm(None)
        return

    def testResourceControlUpdater(self):
        config = self.getConfig()
        resourceUpdater = ResourceControlUpdater(config)
        resourceUpdater.setup(None)
        # TODO: actually run the resource updater algorithm
        return

if __name__ == '__main__':
    unittest.main()