#!/usr/bin/python
"""
_FeederManager_t_

Unit tests for the FeederManager_t.
"""

__revision__ = "$Id: FeederManager_t.py,v 1.3 2010/02/05 21:11:48 meloam Exp $"
__version__ = "$Revision: 1.3 $"

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
                         ['WMComponent.FeederManager.Database',
                          'WMCore.ThreadPool',
                          'WMCore.MsgService',
                          'WMCore.WMBS'],
                                useDefault = False)


    def tearDown(self):
        """
        _tearDown_

        Database deletion
        """
        self.testInit.clearDatabase(['WMComponent.FeederManager.Database',
                                                 'WMCore.ThreadPool',
                                                 'WMCore.MsgService',
                                                 'WMCore.WMBS'])

    def getConfig(self):
        """
        _getConfig_

        Get defaults FeederManager parameters
        """
        return self.testInit.getConfiguration(
                    os.path.join(WMCore.WMInit.getWMBASE(), \
            'src/python/WMComponent/FeederManager/DefaultConfig.py'))


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
                feederManagerdict = {'payload':{'FeederType':'NO Feeder', \
                          'dataset' : 'NO DATASET'}}
                testFeederManager.handleMessage( type = 'AddDatasetWatch' \
                        , payload = feederManagerdict )

        time.sleep(30)

        myThread.workerThreadManager.terminateWorkers()

        while threading.activeCount() > 1:
            print('Currently: '+str(threading.activeCount())+\
                ' Threads. Wait until all our threads have finished')
            time.sleep(1)


if __name__ == "__main__":
    unittest.main()
        
