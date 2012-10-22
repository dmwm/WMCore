#!/usr/bin/python
"""
_FeederManager_t_

Unit tests for the FeederManager_t.
"""

import time
import unittest
import os
import threading
from WMComponent.FeederManager.FeederManager import FeederManager
import WMCore.WMInit
from WMQuality.TestInit import TestInit

class FeederManagerTest(unittest.TestCase):
    """
    TestCase for TestFeederManager module
    """

    _maxMessage = 10

    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Try to create all needed
        tables.
        """

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.generateWorkDir()
        self.testInit.setSchema(customModules = \
                         ['WMCore.Agent.Database',
                          'WMComponent.FeederManager.Database',
                          'WMCore.ThreadPool',
                          'WMCore.WMBS'],
                                useDefault = False)

        return

    def tearDown(self):
        """
        _tearDown_

        Database deletion
        """
        self.testInit.clearDatabase()

        return

    def getConfig(self):
        """
        _createConfig_

        Create a config for the JobAccountant.  This config needs to include
        information for connecting to the database as the component will create
        it's own database connections.  These parameters are still pulled from
        the environment.
        """
        config = self.testInit.getConfiguration()
        self.testInit.generateWorkDir(config)

        config.component_("FeederManager")
        config.FeederManager.logLevel = "INFO"
        config.FeederManager.componentName = "FeederManager"
        config.FeederManager.componentDir = \
            os.path.join(os.getenv("TESTDIR"), "FeederManager")
        config.FeederManager.addDatasetWatchHandler = \
            'WMComponent.FeederManager.Handler.DefaultAddDatasetWatch'

        # The maximum number of threads to process each message type
        config.FeederManager.maxThreads = 10

        # The poll interval at which to look for new fileset/feeder association
        config.FeederManager.pollInterval = 60

        return config

    def testA(self):
        """
        _testA_

        Handle AddDatasetWatch events
        """
        myThread = threading.currentThread()
        config = self.getConfig()
        testFeederManager = FeederManager(config)
        testFeederManager.prepareToStart()

        for i in xrange(0, FeederManagerTest._maxMessage):
            for j in xrange(0, 3):
                feederManagerdict = {'payload':{'FeederType':'NO Feeder',
                                     'dataset' : 'NO DATASET', 'FileType' : 'NO FILE TYPE',
                                     'StartRun' : 'NO START RUN' }}

                testFeederManager.handleMessage( type = 'AddDatasetWatch',
                                                 payload = feederManagerdict )

        time.sleep(30)

        myThread.workerThreadManager.terminateWorkers()

        while threading.activeCount() > 1:
            print('Currently: '+str(threading.activeCount())+\
                ' Threads. Wait until all our threads have finished')
            time.sleep(1)


if __name__ == "__main__":
    unittest.main()
