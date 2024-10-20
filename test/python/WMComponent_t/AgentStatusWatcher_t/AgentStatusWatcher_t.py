#!/usr/bin/env python

"""
AgentStatusWatcher test
"""
import os
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
        # change between RPM and Docker based tests
        print(f"WMA_DEPLOY_DIR: {os.environ.get('WMA_DEPLOY_DIR')}")
        print(f"WMCORE_ROOT: {os.environ.get('WMCORE_ROOT')}")
        if os.environ.get('WMA_DEPLOY_DIR'):
            ## check for Alma9 docker
            print(f"Found WMA_DEPLOY_DIR set to: {os.environ.get('WMA_DEPLOY_DIR')}")
        elif os.environ.get('TEST_DIR'):
            # check for CC7 docker
            print(f"Found TEST_DIR set to: {os.environ.get('TEST_DIR')}")
            os.environ["WMA_DEPLOY_DIR"] = os.path.join(os.environ.get('TEST_DIR'), "WMCore")
        elif os.environ.get('WMCORE_ROOT'):
            # check for CC7 RPM
            print(f"Found WMCORE_ROOT set to: {os.environ.get('WMCORE_ROOT')}")
            #os.environ["WMA_DEPLOY_DIR"] = os.path.join(os.environ.get('WMCORE_ROOT'), "install")
            os.environ["WMA_DEPLOY_DIR"] = os.environ.get('WMCORE_ROOT')
        else:
            # last fallback path
            fallbackDir = "/home/cmsbld/WMCore/"
            print("Did not find any of the expected env vars: WMA_DEPLOY_DIR, TEST_DIR, WMCORE_ROOT")
            print(f"Setting WMA_DEPLOY_DIR environment variable to: {fallbackDir}")
            os.environ["WMA_DEPLOY_DIR"] = os.path.join(fallbackDir, "install")

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
        print(f"Agent Info:\n{agInfo}")

if __name__ == '__main__':
    unittest.main()
