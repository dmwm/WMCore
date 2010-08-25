#!/usr/bin/env python
"""
pullWork poller
"""
__all__ = []
__revision__ = "$Id: WorkQueueManagerWMBSFileFeeder.py,v 1.4 2010/07/29 21:37:49 sryu Exp $"
__version__ = "$Revision: 1.4 $"


import threading

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.ResourceControl.ResourceControl import ResourceControl
from WMCore.DAOFactory import DAOFactory
from WMCore.WMBS.Job import Job

class WorkQueueManagerWMBSFileFeeder(BaseWorkerThread):
    """
    Polls for Work
    """
    def __init__(self, queue):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        
        self.queue = queue
        
        self.previousWorkList = []
        
        self.resourceControl = ResourceControl()
        
    def algorithm(self, parameters):
        """
        Pull in work
        """
        # reinitialize site and slot
        if self.checkJobCreation():
            self.getWorks()
        
    def getWorks(self):
        
        
        self.queue.logger.info("Getting work and feeding WMBS files")

        # need to make sure jobs are created
        siteRCDict = self.resourceControl.listThresholdsForCreate()
        
        workQueueDict = {}

        for site in siteRCDict.keys():
            #This is the number of free slots
            # - the number of Created but not Exectuing jobs
            freeSlots = siteRCDict[site]['total_slots'] - siteRCDict[site]['running_jobs'] 
            #I need freeSlots jobs on site location
            self.queue.logger.info('I need %s jobs on site %s' %(freeSlots, site))

            if freeSlots < 0:
                freeSlots = 0
            workQueueDict[site] = freeSlots

        self.previousWorkList = self.queue.getWork(workQueueDict)
        
        return

    def checkJobCreation(self):
        # check to see whether there is job created for all the file 
        # in the given subscription
        self.queue.logger.info("Checking the JobCreation from previous pulled work")
        for workUnit in self.previousWorkList:
            if len(workUnit["subscription"].filesOfStatus("Available")) > 0:
                self.queue.logger.info("Not all the jobs are created.\nWill get the work later")
                return False
        
        self.queue.logger.info("All the jobs are created.\nWill get the work now")
        #reset previousWorkList to [] since all the jobs are created
        self.previousWorkList = []
        return True
        
        
        