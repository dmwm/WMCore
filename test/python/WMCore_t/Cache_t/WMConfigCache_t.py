#!/usr/bin/env python
"""
_WMConfigCache_t_

Test class for the WMConfigCache
"""

from future import standard_library
standard_library.install_aliases()
import urllib.parse

import os
import unittest
import tempfile
import subprocess

from WMCore.Agent.Configuration import Configuration
from WMCore.Cache.WMConfigCache import ConfigCache, ConfigCacheException
from WMCore.WMBase import getTestBase
from WMQuality.TestInitCouchApp import TestInitCouchApp

class testWMConfigCache(unittest.TestCase):
    """
    _testWMConfigCache_

    Basic test class for configCache
    """
    def setUp(self):
        """
        _setUp_

        """
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setupCouch("config_test", "GroupUser", "ConfigCache")

        self.testDir = self.testInit.generateWorkDir()
        return

    def tearDown(self):
        """
        _tearDown_

        Clear out the database.
        """
        self.testInit.delWorkDir()
        self.testInit.tearDownCouch()
        return

    def testA_basicConfig(self):
        """
        _basicConfig_

        Basic configCache stuff.
        """
        PSetTweak = "Hello, I am a PSetTweak.  It's nice to meet you."

        configCache = ConfigCache(os.environ["COUCHURL"], couchDBName = 'config_test')
        configCache.createUserGroup(groupname = "testGroup", username = 'testOps')
        configCache.setPSetTweaks(PSetTweak = PSetTweak)
        configCache.save()

        configCache2 = ConfigCache(os.environ["COUCHURL"], couchDBName = 'config_test',
                                   id = configCache.getCouchID(),
                                   rev = configCache.getCouchRev())
        configCache2.loadByID(configCache.getCouchID())

        self.assertEqual(configCache2.getPSetTweaks(), PSetTweak)

        configCache2.delete()
        configCache3 = ConfigCache(os.environ["COUCHURL"], couchDBName = 'config_test',
                                   id = configCache.getCouchID(),
                                   rev = configCache.getCouchRev())

        testFlag = False

        # It should fail to load deleted documents
        try:
            configCache3.loadByID(configCache.getCouchID())
        except ConfigCacheException:
            testFlag = True

        self.assertTrue(testFlag)

        return

    def testB_addingConfigsAndTweaks(self):
        """
        _addingConfigsAndTweaks_

        Test adding config files and tweak files
        """
        PSetTweak = "Hello, I am a PSetTweak.  It's nice to meet you."
        attach    = "Hello, I am an attachment"

        configCache = ConfigCache(os.environ["COUCHURL"], couchDBName = 'config_test')
        configCache.createUserGroup(groupname = "testGroup", username = 'testOps')
        configCache.setPSetTweaks(PSetTweak = PSetTweak)
        configCache.attachments['attach1'] = attach
        psetPath = os.path.join(getTestBase(), "WMCore_t/Cache_t/PSet.txt")
        psetPath = "file://" + urllib.parse.quote(os.path.abspath(psetPath))
        configCache.addConfig(newConfig = psetPath, psetHash = None)

        configCache.setLabel("sample-label")
        configCache.setDescription("describe this config here")
        configCache.save()
        configString1 = configCache.getConfig()

        configCache2 = ConfigCache(os.environ["COUCHURL"], couchDBName = 'config_test',
                                   id = configCache.getCouchID(),
                                   rev = configCache.getCouchRev())
        configCache2.loadByID(configCache.getCouchID())
        configString2 = configCache2.getConfig()

        self.assertEqual(configString1, configString2)
        self.assertEqual(configCache2.attachments.get('attach1', None), attach)

        configCache.delete()
        return


    def testC_testViews(self):
        """
        _testViews_

        Prototype test for what should be a lot of other tests.
        """
        PSetTweak = "Hello, I am a PSetTweak.  It's nice to meet you."
        attach    = "Hello, I am an attachment"

        configCache = ConfigCache(os.environ["COUCHURL"], couchDBName = 'config_test')
        configCache.createUserGroup(groupname = "testGroup", username = 'testOps')
        configCache.setPSetTweaks(PSetTweak = PSetTweak)
        configCache.attachments['attach1'] = attach
        configCache.document['md5_hash'] = "somemd5"
        psetPath = os.path.join(getTestBase(), "WMCore_t/Cache_t/PSet.txt")
        psetPath = "file://" + urllib.parse.quote(os.path.abspath(psetPath))
        configCache.addConfig(newConfig = psetPath, psetHash = None)
        configCache.save()

        configCache2 = ConfigCache(os.environ["COUCHURL"], couchDBName = 'config_test')
        configCache2.document['md5_hash'] = configCache.document['md5_hash']
        configCache2.load()

        self.assertEqual(configCache2.attachments.get('attach1', None), attach)
        configCache2.delete()
        return

    def testD_LoadConfigCache(self):
        """
        _LoadConfigCache_

        Actually load the config cache using plain .load()
        Tests to make sure that if we pass in an id field it gets used to load configs
        """

        configCache = ConfigCache(os.environ["COUCHURL"], couchDBName = 'config_test')
        configCache.createUserGroup(groupname = "testGroup", username = 'testOps')
        configCache.setLabel("labelA")
        configCache.save()

        configCache2 = ConfigCache(os.environ["COUCHURL"], couchDBName = 'config_test',
                                   id = configCache.getCouchID(),
                                   rev = configCache.getCouchRev())
        configCache2.load()
        self.assertEqual(configCache2.document['owner'],
                         {'group': 'testGroup', 'user': 'testOps'})
        self.assertEqual(configCache2.document['description'],
                         {'config_desc': None, 'config_label': 'labelA'})
        return

    def testE_SaveConfigFileToDisk(self):
        """
        _SaveConfigFileToDisk_

        Check and see if we can save the config file attachment to disk
        """
        targetFile = os.path.join(self.testDir, 'configCache.test')

        configCache = ConfigCache(os.environ["COUCHURL"], couchDBName = 'config_test')
        configCache.createUserGroup(groupname = "testGroup", username = 'testOps')
        configCache.attachments['configFile'] = 'ThisIsAConfigFile'
        configCache.saveConfigToDisk(targetFile = targetFile)

        f = open(targetFile, 'r')
        content = f.read()
        f.close()

        self.assertEqual(content, configCache.getConfig())
        return

    def testListAllConfigs(self):
        """
        _testListAllConfigs_

        Verify that the list all configs method works correctly.
        """
        configCacheA = ConfigCache(os.environ["COUCHURL"], couchDBName = 'config_test')
        configCacheA.createUserGroup(groupname = "testGroup", username = 'testOps')
        configCacheA.setLabel("labelA")
        configCacheA.save()

        configCacheB = ConfigCache(os.environ["COUCHURL"], couchDBName = 'config_test')
        configCacheB.createUserGroup(groupname = "testGroup", username = 'testOps')
        configCacheB.setLabel("labelB")
        configCacheB.save()

        configs = configCacheA.listAllConfigsByLabel()

        self.assertEqual(len(configs), 2,
                         "Error: There should be two configs")
        self.assertEqual(configs["labelA"], configCacheA.getCouchID(),
                         "Error: Label A is wrong.")
        self.assertEqual(configs["labelB"], configCacheB.getCouchID(),
                         "Error: Label B is wrong.")
        return

if __name__ == "__main__":
    unittest.main()
