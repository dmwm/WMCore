#!/usr/bin/python
"""
_WorkflowManager_t_

Unit tests for the WorkflowManager_t.
"""




import time
import unittest
import os
import threading
import nose

from WMComponent.WorkflowManager.WorkflowManager import WorkflowManager
from WMCore.WMBS.Workflow import Workflow

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
                     ['WMCore.Agent.Database',
                      'WMComponent.WorkflowManager.Database',
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
        _getConfig_

        Get defaults WorkflowManager parameters
        """

        return self.testInit.getConfiguration(
                    os.path.join(WMCore.WMInit.getWMBASE(), \
           'src/python/WMComponent/WorkflowManager/DefaultConfig.py'))


    def testA(self):
        """
        _testA_

        Handle AddWorkflowToManage events
        """
        myThread = threading.currentThread()
        config = self.getConfig()

        testWorkflowManager = WorkflowManager(config)
        testWorkflowManager.prepareToStart()

        for i in xrange(0, WorkflowManagerTest._maxMessage):

            workflow = Workflow(spec = "testSpec.xml", owner = "riahi", \
               name = "testWorkflow" + str(i), task = "testTask")
            workflow.create()

            for j in xrange(0, 3):
                workflowManagerdict = {'payload':{'WorkflowId' : workflow.id \
          , 'FilesetMatch': 'FILESET_' + str(j) ,'SplitAlgo':'NO SPLITALGO', 'Type':'NO TYPE' }}
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

