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





class testWMConfigCache(unittest.TestCase):
    """
    _testWMConfigCache_

    Basic test class for configCache
    """




    def setUp(self):
        """
        Minimal setup

        """

        pipe = subprocess.Popen('sed -i \"1i\\ # hello\" PSet.py', stdout = subprocess.PIPE,
                                stderr = subprocess.PIPE,
                                shell = True)


        return


    def tearDown(self):
        """
        Minimal tear down

        """

        return


    def createConfig(self):
        """



        """

        config = Configuration()

        config.section_("CoreDatabase")
        config.CoreDatabase.couchurl = os.getenv('COUCHURL', 'http://dmwmwriter:gutslap!@cmssrv52.fnal.gov:59')

        return config


    def testA_basicConfig(self):
        """
        _basicConfig_

        Basic configCache stuff.
        """

        config = self.createConfig()

        PSetTweak = "Hello, I am a PSetTweak.  It's nice to meet you."


        configCache = ConfigCache(config = config, couchDBName = 'config_test')
        configCache.createUserGroup(groupname = "testGroup", username = 'testOps')
        configCache.setPSetTweaks(PSetTweak = PSetTweak)
        configCache.save()




        configCache2 = ConfigCache(config = config, couchDBName = 'config_test',
                                   id = configCache.getCouchID(),
                                   rev = configCache.getCouchRev())
        configCache2.loadByID()

        self.assertEqual(configCache2.getPSetTweaks(), PSetTweak)


        configCache2.delete()

        configCache3 = ConfigCache(config = config, couchDBName = 'config_test',
                                   id = configCache.getCouchID(),
                                   rev = configCache.getCouchRev())

        testFlag = False

        # It should fail to load deleted documents
        try:
            configCache3.loadByID()
        except ConfigCacheException:
            testFlag = True

        self.assertTrue(testFlag)

        return



    def testB_addingConfigsAndTweaks(self):
        """
        _addingConfigsAndTweaks_
        
        Test adding config files and tweak files
        """

        
        config = self.createConfig()

        PSetTweak = "Hello, I am a PSetTweak.  It's nice to meet you."
        attach    = "Hello, I am an attachment"


        configCache = ConfigCache(config = config, couchDBName = 'config_test')
        configCache.createUserGroup(groupname = "testGroup", username = 'testOps')
        configCache.setPSetTweaks(PSetTweak = PSetTweak)
        configCache.attachments['attach1'] = attach
        configCache.save()

        configCache.addConfig(newConfig = 'PSet.py')

        configString1 = configCache.getConfig()



        configCache2 = ConfigCache(config = config, couchDBName = 'config_test',
                                   id = configCache.getCouchID(),
                                   rev = configCache.getCouchRev())
        configCache2.loadByID()

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


        config = self.createConfig()

        PSetTweak = "Hello, I am a PSetTweak.  It's nice to meet you."
        attach    = "Hello, I am an attachment"


        configCache = ConfigCache(config = config, couchDBName = 'config_test')
        configCache.createUserGroup(groupname = "testGroup", username = 'testOps')
        configCache.setPSetTweaks(PSetTweak = PSetTweak)
        configCache.attachments['attach1'] = attach
        configCache.save()

        configCache.addConfig(newConfig = 'PSet.py')

        
        configCache2 = ConfigCache(config = config, couchDBName = 'config_test')
        configCache2.document['md5_hash'] = configCache.document['md5_hash']
        configCache2.load()
        
        #configCache2.loadByMD5(md5 = configCache.document['md5_hash'])

        self.assertEqual(configCache2.attachments.get('attach1', None), attach)


        # This shouldn't work yet.
        configCache3 = ConfigCache(config = config, couchDBName = 'config_test')
        configCache3.createUserGroup(groupname = "testGroup", username = 'testOps')
        configCache3.load()

        self.assertEqual(configCache3.id, None)

        configCache2.delete()



if __name__ == "__main__":
    unittest.main() 
