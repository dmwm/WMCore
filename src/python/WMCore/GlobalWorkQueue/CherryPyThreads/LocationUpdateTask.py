from __future__ import (division, print_function)
from time import time
from WMCore.REST.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.WorkQueue.WorkQueue import globalQueue

class LocationUpdateTask(CherryPyPeriodicTask):

    def __init__(self, rest, config):

        super(LocationUpdateTask, self).__init__(config)
        self.globalQ = globalQueue(logger=self.logger, **config.queueParams)

    def setConcurrentTasks(self, config):
        """
        sets the list of function reference for concurrent tasks
        """
        self.concurrentTasks = [{'func': self.updateDataLocation, 'duration': config.locationUpdateDuration}]

    def updateDataLocation(self, config):
        """
        gather active data statistics
        """
        tStart = time()
        res = self.globalQ.updateLocationInfo()
        self.logger.info("%s executed in %.3f secs and updated %d non-unique elements",
                         self.__class__.__name__, time() - tStart, res)

        return
