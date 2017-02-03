from __future__ import (division, print_function)

from WMCore.REST.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.WorkQueue.WorkQueue import globalQueue

class LocationUpdateTask(CherryPyPeriodicTask):

    def __init__(self, rest, config):

        super(LocationUpdateTask, self).__init__(config)

    def setConcurrentTasks(self, config):
        """
        sets the list of function reference for concurrent tasks
        """
        self.concurrentTasks = [{'func': self.updateDataLocation, 'duration': config.locationUpdateDuration}]

    def updateDataLocation(self, config):
        """
        gather active data statistics
        """
        
        globalQ = globalQueue(**config.queueParams)
        globalQ.updateLocationInfo()
                  
        return
