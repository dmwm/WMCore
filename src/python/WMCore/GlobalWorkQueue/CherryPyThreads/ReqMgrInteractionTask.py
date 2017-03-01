from __future__ import (division, print_function)

from WMCore.REST.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.WorkQueue.WorkQueue import globalQueue
from WMCore.WorkQueue.WorkQueueReqMgrInterface import WorkQueueReqMgrInterface

class ReqMgrInteractionTask(CherryPyPeriodicTask):

    def __init__(self, rest, config):

        super(ReqMgrInteractionTask, self).__init__(config)

    def setConcurrentTasks(self, config):
        """
        sets the list of function reference for concurrent tasks
        """
        self.concurrentTasks = [{'func': self.interactWithReqmgr, 'duration': config.interactDuration}]

    def interactWithReqmgr(self, config):
        """

        1. pull new work
        2. add the new element from running-open request
        3. report element status to reqmgr (need to be removed and set as reqmgr task)
        4. record this activity
        """

        globalQ = globalQueue(**config.queueParams)
        reqMgrInt = WorkQueueReqMgrInterface(**config.reqMgrConfig)
        reqMgrInt(globalQ)

        return
