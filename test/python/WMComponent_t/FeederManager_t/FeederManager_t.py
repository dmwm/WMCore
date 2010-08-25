#!/usr/bin/python
"""
_FeederManager_t_

Unit tests for the FeederManager_t.
"""

__revision__ = "$Id: FeederManager_t.py,v 1.5 2010/02/11 19:23:54 meloam Exp $"
__version__ = "$Revision: 1.5 $"

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
        raise RuntimeError, "This test hangs. please fix me"
# this is the message before it hangs
#    MComponent_t.FeederManager_t.FeederManager_t.FeederManagerTest.testA -- _testA_ ... Exception in thread Thread-2:
#Traceback (most recent call last):
#  File "/home/bbslave/shared/python26/lib/python2.6/threading.py", line 522, in __bootstrap_inner
#    self.run()
#  File "/home/bbslave/shared/python26/lib/python2.6/threading.py", line 477, in run
#    self.__target(*self.__args, **self.__kwargs)
#  File "/home/bbslave/buildslave/full-sl5-x86_64-python26-mysql/build/src/python/WMCore/ThreadPool/ThreadPool.py", line 228, in slaveThread
#    results = slaveServer( *parameters )
#  File "/home/bbslave/buildslave/full-sl5-x86_64-python26-mysql/build/src/python/WMComponent/FeederManager/Handler/DefaultAddDatasetWatchSlave.py", line 46, in __call__
#    fileType = message["FileType"]
#KeyError: 'FileType'
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
        
