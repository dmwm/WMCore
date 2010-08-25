#!/usr/bin/python
"""
_WorkflowManager_t_

Unit tests for the WorkflowManager_t.
"""

__revision__ = "$Id: WorkflowManager_t.py,v 1.2 2010/02/04 22:36:34 meloam Exp $"
__version__ = "$Revision: 1.2 $"

import time
import unittest
import os
import threading

from WMComponent.WorkflowManager.WorkflowManager import WorkflowManager

from WMQuality.TestInit import TestInit

import WMCore.WMInit
class WorkflowManagerTest(unittest.TestCase):
    """
    TestCase for TestWorkflowManager module
    """

    _maxMessage = 10
 
    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Try to create all needed 
        WMBS tables.  
        """

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = \
                     ['WMComponent.WorkflowManager.Database',
                      'WMCore.ThreadPool',
                      'WMCore.MsgService',
                      'WMCore.WMBS'],
                    useDefault = False)


    def tearDown(self):
        """
        _tearDown_

        Database deletion
        """
        self.testInit.clearDatabase([\
    'WMComponent.WorkflowManager.Database', 
    'WMCore.ThreadPool', 'WMCore.MsgService', 
         'WMCore.WMBS'])

    def getConfig(self):
        """
        _getConfig_

        Get defaults WorkflowManager parameters
        """

        return self.testInit.getConfiguration(
                    os.path.join(WMCore.WMInit.getWMBASE(), \
           'src/python/WMComponent/WorkflowManager/DefaultConfig.py'))


    def testA(self):
        """
        _testA_

        Handle malformed AddWorkflowToManage events  
        """
        myThread = threading.currentThread()
        config = self.getConfig()

        testWorkflowManager = WorkflowManager(config)
        testWorkflowManager.prepareToStart()

        for i in xrange(0, WorkflowManagerTest._maxMessage):
            for j in xrange(0, 3):
                workflowManagerdict = {'payload':{'WorkflowId' : 'NO ID' \
          , 'FilesetMatch': 'NO FILESET' ,'SplitAlgo':'NO SPLITALGO' }} 
                testWorkflowManager.handleMessage( \
      type = 'AddWorkflowToManage' , payload = workflowManagerdict )

        time.sleep(30)

        myThread.workerThreadManager.terminateWorkers()

        while threading.activeCount() > 1:
            print('Currently: '+str(threading.activeCount())+\
                ' Threads. Wait until all our threads have finished')
            time.sleep(1)


if __name__ == "__main__":
    unittest.main()
        
