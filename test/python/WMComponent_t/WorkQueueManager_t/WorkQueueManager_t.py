#!/usr/bin/env python

"""
WorkQueuManager test
"""
from __future__ import print_function, division

from builtins import next
import unittest

from WMCore_t.WorkQueue_t.WorkQueueTestCase import WorkQueueTestCase

from WMCore.WorkQueue.WorkQueue import globalQueue


def getFirstTask(wmspec):
    """Return the 1st top level task"""
    return next(wmspec.taskIterator())


class WorkQueueManagerTest(WorkQueueTestCase):
    """
    TestCase for WorkQueueManagerTest module
    """

    _maxMessage = 10

    def setSchema(self):
        self.schema = []
        self.couchApps = ["WorkQueue"]

    def getConfig(self):
        """
        _createConfig_

        General config file
        """
        # configPath=os.path.join(WMCore.WMInit.getWMBASE(), \
        #                        'src/python/WMComponent/WorkQueueManager/DefaultConfig.py')):


        config = self.testInit.getConfiguration()
        # http://www.logilab.org/ticket/8961
        # pylint: disable=E1101, E1103
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

    def setupGlobalWorkqueue(self):
        """Return a workqueue instance"""

        globalQ = globalQueue(CacheDir=self.workDir,
                              QueueURL='global.example.com',
                              Teams=["The A-Team", "some other bloke"],
                              DbName='workqueue_t_global')
        return globalQ

    def testComponentBasic(self):
        """
        Tests the components, as in sees if they load.
        Otherwise does nothing.
        """
        return
        # TODO: What used to be here, stopping pylint from complaining about unreachable code
        # myThread = threading.currentThread()
        #
        # config = self.getConfig()
        #
        # testWorkQueueManager = WorkQueueManager(config)
        # testWorkQueueManager.prepareToStart()
        #
        # time.sleep(30)
        # print("Killing")
        # myThread.workerThreadManager.terminateWorkers()
        #
        # return


if __name__ == '__main__':
    unittest.main()
