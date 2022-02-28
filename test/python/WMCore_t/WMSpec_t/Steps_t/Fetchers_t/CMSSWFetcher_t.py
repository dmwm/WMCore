#!/usr/bin/env python

"""
CMSSWFetcher

A unittest for seeing if we can pull code the configCache to the WN
Written by someone who has no idea what CMSSWFetcher is supposed to do.
"""
import os
import os.path
import unittest

from WMCore.WMSpec import WMTask
from WMCore.WMSpec import WMStep
from WMCore.WMSpec.Steps.Fetchers.CMSSWFetcher import CMSSWFetcher
from WMQuality.TestInitCouchApp                import TestInitCouchApp as TestInit
from WMCore.Cache.WMConfigCache                import ConfigCache

class CMSSWFetcherTest(unittest.TestCase):
    """
    Main test for the URLFetcher

    """

    def setUp(self):
        """
        Basic setUp

        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setupCouch("config_test", "GroupUser", "ConfigCache")

        self.testDir = self.testInit.generateWorkDir()

        return

    def tearDown(self):
        """
        Basic tearDown

        """
        self.testInit.delWorkDir()
        self.testInit.tearDownCouch()
        return

    def getConfig(self):
        """
        _getConfig_

        Create a test config and put it in the cache
        """
        PSetTweak = {'someKey': "Hello, I am a PSetTweak.  It's nice to meet you."}

        configCache = ConfigCache(os.environ["COUCHURL"], couchDBName = 'config_test')
        configCache.createUserGroup(groupname = "testGroup", username = 'testOps')
        configCache.setPSetTweaks(PSetTweak = PSetTweak)
        configCache.attachments['configFile'] = 'This Is A Test Config'
        configCache.save()
        return configCache


    def createTask(self, configCache):
        """
        _createTask_

        Create a test task that includes the
        fileURL
        """

        task = WMTask.makeWMTask("testTask")
        task.makeStep("step1")
        task.makeStep("step2")

        for t in task.steps().nodeIterator():
            t = WMStep.WMStepHelper(t)
            os.mkdir(os.path.join(self.testDir, t.name()))
            t.setStepType("CMSSW")
            t.data.application.section_('command')
            t.data.application.configuration.configCacheUrl = configCache.dburl
            t.data.application.configuration.cacheName      = configCache.dbname
            t.data.application.configuration.configId       = configCache.getCouchID()
            t.data.application.command.psetTweak            = 'tweak'
            t.data.application.command.configuration        = 'configCache.file'
        return task

    def testA_BasicFunction(self):
        """
        _BasicFunction_

        Run a test to find out if we can grab a configCache
        """
        configCache = self.getConfig()
        task        = self.createTask(configCache = configCache)
        fetcher     = CMSSWFetcher()
        fetcher.setWorkingDirectory(workingDir = self.testDir)
        self.assertEqual(fetcher.workingDirectory(), self.testDir)

        fetcher(wmTask = task)
        configFilePath = os.path.join(self.testDir, 'step2', 'configCache.file')
        self.assertTrue(os.path.isfile(configFilePath))

        with open(configFilePath) as f:
            content = f.read()

        self.assertEqual(content, 'This Is A Test Config')
        return

if __name__ == "__main__":
    unittest.main()
