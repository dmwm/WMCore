'''
Created on May 19, 2015

'''

from __future__ import (division, print_function)

from Utils.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.ReqMgr.Service.Auxiliary import update_software

class UpdateAuxDBTasks(CherryPyPeriodicTask):

    def __init__(self, rest, config):

        CherryPyPeriodicTask.__init__(self, config)

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