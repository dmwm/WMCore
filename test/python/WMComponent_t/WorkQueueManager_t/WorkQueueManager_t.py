#!/usr/bin/env python

"""
JobArchiver test 
"""

__revision__ = "$Id: WorkQueueManager_t.py,v 1.2 2010/02/04 22:36:36 meloam Exp $"
__version__ = "$Revision: 1.2 $"

import os
import logging
import threading
import unittest
import time
import shutil
import WMCore.WMInit
from subprocess import Popen, PIPE

from WMCore.Agent.Configuration import loadConfigurationFile



from WMQuality.TestInit   import TestInit

from WMComponent.WorkQueueManager.WorkQueueManager import WorkQueueManager


class WorkQueueManagerTest(unittest.TestCase):
    """
    TestCase for WorkQueueManagerTest module 
    """


    _maxMessage = 10

    def setUp(self):
        """
        setup for test.
        """

        myThread = threading.currentThread()
        
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        #self.tearDown()
        self.testInit.setSchema(customModules = ["WMCore.WorkQueue.Database", "WMCore.WMBS", 
                                                 "WMCore.MsgService", "WMCore.ThreadPool"],
                                useDefault = False)

    def tearDown(self):
        """
        Database deletion
        """

        self.testInit.clearDatabase()


    def getConfig(self):
        """
        _createConfig_

        General config file
        """
        #configPath=os.path.join(WMCore.WMInit.getWMBASE(), \
        #                        'src/python/WMComponent/WorkQueueManager/DefaultConfig.py')):


        config = self.testInit.getConfiguration()
        
        config.component_("WorkQueueManager")
        config.section_("General")
        config.General.workDir = "."
        config.WorkQueueManager.team = 'team_usa'
        config.WorkQueueManager.requestMgrHost = 'cmssrv49.fnal.gov:8585'
        config.WorkQueueManager.serviceUrl = "http://cmssrv18.fnal.gov:6660"
        
        config.WorkQueueManager.logLevel = 'INFO'
        config.WorkQueueManager.pollInterval = 10
        config.WorkQueueManager.level = "GlobalQueue"
        return config        
        


    def testComponentBasic(self):
        """
        Tests the components, as in sees if they load.
        Otherwise does nothing.
        """
        myThread = threading.currentThread()

        config = self.getConfig()

        testWorkQueueManager = WorkQueueManager(config)
        testWorkQueueManager.prepareToStart()
        
        time.sleep(30)
        print "Killing"
        myThread.workerThreadManager.terminateWorkers()

        return

if __name__ == '__main__':
    unittest.main()

