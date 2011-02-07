#!/usr/bin/env python
"""
_WMConfigCache_t_

Test class for the WMConfigCache
"""

import os
import unittest
import tempfile
import subprocess

from WMCore.Agent.Configuration import Configuration
from WMCore.Cache.WMConfigCache import ConfigCache, ConfigCacheException
from WMCore.WMInit import getWMBASE
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
        return

    def tearDown(self):
        """
        _tearDown_

        Clear out the database.
        """
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
        psetPath = os.path.join(getWMBASE(), "test/python/WMCore_t/Cache_t/PSet.txt")
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
        psetPath = os.path.join(getWMBASE(), "test/python/WMCore_t/Cache_t/PSet.txt")        
        configCache.addConfig(newConfig = psetPath, psetHash = None)        
        configCache.save()
        
        configCache2 = ConfigCache(os.environ["COUCHURL"], couchDBName = 'config_test')
        configCache2.document['md5_hash'] = configCache.document['md5_hash']
        configCache2.load()
        
        self.assertEqual(configCache2.attachments.get('attach1', None), attach)
        configCache2.delete()
        return

if __name__ == "__main__":
    unittest.main() 
