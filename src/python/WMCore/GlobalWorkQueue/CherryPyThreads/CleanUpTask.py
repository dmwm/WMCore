from __future__ import (division, print_function)

from time import time
from WMCore.REST.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.WorkQueue.WorkQueue import globalQueue

class CleanUpTask(CherryPyPeriodicTask):

    def __init__(self, rest, config):

        super(CleanUpTask, self).__init__(config)
        self.globalQ = globalQueue(logger=self.logger, **config.queueParams)

    def setConcurrentTasks(self, config):
        """
        sets the list of function reference for concurrent tasks
        """
        self.concurrentTasks = [{'func': self.cleanUpAndSyncCanceledElements, 'duration': config.cleanUpDuration}]

    def cleanUpAndSyncCanceledElements(self, config):
        """
        1. deleted the wqe in end states
        2. synchronize cancelled elements.
        We can also make this in the separate thread
        """
        tStart = time()
        self.globalQ.performQueueCleanupActions(skipWMBS=True)
        self.logger.info("%s executed in %.3f secs.", self.__class__.__name__, time() - tStart)
        return
