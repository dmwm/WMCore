'''
Created on May 19, 2015

'''

from __future__ import (division, print_function)

from WMCore.REST.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.ReqMgr.Service.Auxiliary import update_software

class UpdateAuxDBTasks(CherryPyPeriodicTask):

    def __init__(self, rest, config):

        super(UpdateAuxDBTasks, self).__init__(config)

    def setConcurrentTasks(self, config):
        """
        sets the list of functions which
        """
        self.concurrentTasks = [{'func': self.updateCMSSW, 'duration': config.cupdateCMSSWDuration}]

    def updateCMSSW(self, config):
        """
        gather active data statistics
        """
        
        update_software(config)