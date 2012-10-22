#!/usr/bin/env python
"""
pullWork poller
"""
__all__ = []




import time
import random

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.WorkQueue.WMBSHelper import freeSlots
from WMCore.DAOFactory import DAOFactory
from WMCore.WMBS.Job import Job
from WMCore.WorkQueue.WorkQueueUtils import cmsSiteNames

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

    def setup(self, parameters):
        """
        Called at startup - introduce random delay
             to avoid workers all starting at once
        """
        t = random.randrange(self.idleTime)
        self.logger.info('Sleeping for %d seconds before 1st loop' % t)
        time.sleep(t)

    def algorithm(self, parameters):
        """
        Pull in work
        """
        # reinitialize site and slot
        if self.checkJobCreation():
            try:
                self.getWorks()
            except Exception, ex:
                self.queue.logger.error("Error in wmbs inject loop: %s" % str(ex))

    def getWorks(self):
        """
        Inject work into wmbs for idle sites
        """
        self.queue.logger.info("Getting work and feeding WMBS files")

        # need to make sure jobs are created
        resources = freeSlots(minusRunning = True, allowedStates = ['Normal', 'Draining'],
                              knownCmsSites = cmsSiteNames())

        for site in resources:
            self.queue.logger.info("I need %d jobs on site %s" % (resources[site], site))

        self.previousWorkList = self.queue.getWork(resources)
        self.queue.logger.info("%s of units of work acquired for file creation"
                               % len(self.previousWorkList))
        return

    def checkJobCreation(self):
        # check to see whether there is job created for all the file
        # in the given subscription
        self.queue.logger.info("Checking the JobCreation from previous pulled work")
        for workUnit in self.previousWorkList:
            filesForPeningJobCreation = len(workUnit["Subscription"].filesOfStatus("Available"))
            if filesForPeningJobCreation > 0:
                self.queue.logger.info("""Not all the jobs are created.
                                          %s files left for job creation
                                          Will get the work later""" %
                                          filesForPeningJobCreation)
                return False

        self.queue.logger.info("All the jobs are created.\nWill get the work now")
        #reset previousWorkList to [] since all the jobs are created
        self.previousWorkList = []
        return True
