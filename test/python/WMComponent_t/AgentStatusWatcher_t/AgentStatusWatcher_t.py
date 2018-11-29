#!/usr/bin/env python

"""
AgentStatusWatcher test
"""

from __future__ import print_function, division

import threading
import unittest
from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit

from WMComponent.AgentStatusWatcher.AgentStatusPoller import AgentStatusPoller


class AgentStatusWatcher_t(unittest.TestCase):
    """
    TestCase for AgentStatustWatcher module
    """

    def setUp(self):
        """
        setup for test.
        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.reqmonDBName = "wmstat_t"
        self.localDBName = "wmstat_t_local"
        self.testInit.setupCouch(self.reqmonDBName, "WMStatsAgent")
        self.testInit.setupCouch(self.localDBName, "WMStatsAgent")
        self.testDir = self.testInit.generateWorkDir()
        return

    def tearDown(self):
        """
        Database deletion
        """
        threading.currentThread()

        self.testInit.delWorkDir()
        self.testInit.tearDownCouch()
        self.testInit.clearDatabase()
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

        config.component_("AgentStatusWatcher")
        config.AgentStatusWatcher.namespace = "WMComponent.AgentStatusWatcher.AgentStatusWatcher"
        config.AgentStatusWatcher.jsonFile = "/dev/null"

        config.component_("AnalyticsDataCollector")
        config.AnalyticsDataCollector.namespace = "WMComponent.AnalyticsDataCollector.AnalyticsDataCollector"
        config.AnalyticsDataCollector.componentDir = config.General.workDir + "/AnalyticsDataCollector"
        config.AnalyticsDataCollector.localQueueURL = "%s/%s" % (couchURL, "workqueue")
        config.AnalyticsDataCollector.summaryLevel = "task"

        return config

    def testAgentStatusPoller(self):
        """
        Tests the components, as in sees if they load.
        Otherwise does nothing.
        """
        threading.currentThread()
        config = self.getConfig()
        watcher = AgentStatusPoller(config)
        # Note:
        # Should call: watcher.setup() and watcher.algorithm()
        # for a complete poller test.
        # Just testing proxy/certificate lifetimes for now.

        # Start with ok status
        agInfo = {'status':'ok'}

        # Check service certificate and proxy lifetimes
        watcher.checkCredLifetime(agInfo, "proxy")
        watcher.checkCredLifetime(agInfo, "certificate")
        print("Agent Info:\n%s" % (agInfo))

if __name__ == '__main__':
    unittest.main()
