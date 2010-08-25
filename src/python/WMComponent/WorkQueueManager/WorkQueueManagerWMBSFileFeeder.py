#!/usr/bin/env python
"""
pullWork poller
"""
__all__ = []
__revision__ = "$Id: WorkQueueManagerWMBSFileFeeder.py,v 1.2 2010/05/13 18:43:34 sryu Exp $"
__version__ = "$Revision: 1.2 $"


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
        myThread = threading.currentThread()
        #DAO factory for WMBS objects
        self.daoFactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = self.queue.logger, 
                                     dbinterface = myThread.dbi)
       
        self.topOffFactor = 1.0
        self.sites = {}
        self.slots = {}
        self.resourceControl = ResourceControl()
        
    def algorithm(self, parameters):
        """
        Pull in work
        """
        # reinitialize site and slot
        myThread = threading.currentThread()
        myThread.name = "WorkQueueManagerWMBSFileFeeder"
        self.sites = {}
        self.slots = {}
        self.pollJobs()
        self.getWorks()
        
    def getWorks(self):
        
        self.queue.logger.info("Getting work and feeding WMBS files")

        workQueueDict = {}

        for location in self.sites.keys():
            #This is the number of free slots
            # - the number of Created but not Exectuing jobs
            freeSlots = (self.slots[location] * self.topOffFactor) \
                        - self.sites[location]

            #I need freeSlots jobs on site location
            self.queue.logger.info('I need %s jobs on site %s' %(freeSlots, location))

            if freeSlots < 0:
                freeSlots = 0
            workQueueDict[location] = freeSlots

        self.queue.getWork(workQueueDict)

        return

        
    
    def pollJobs(self):
        """
        Survey sites for number of open slots; number of running jobs

        """

        siteRCDict = self.resourceControl.listThresholdsForCreate()

        # This should be two tiered: 1st location, 2nd number of slots

        for site in siteRCDict.keys():
            self.sites[site] = siteRCDict[site]['running_jobs']
            self.slots[site] = siteRCDict[site]['total_slots']
            self.queue.logger.info("There are now %s jobs for site %s" \
                         %(self.sites[site], site))


        # Now we have to make some quick guesses about jobs not yet submitted:
        jobAction = self.daoFactory(classname = "Jobs.GetAllJobs")
        jobList   = jobAction.execute(state = 'Created')
        for jobID in jobList:
            job = Job(id = jobID)
            job["location"] = self.findSiteForJob(job)
            self.sites[job["location"]] += 1

    def findSiteForJob(self, job):
        """
        _findSiteForJob_

        This searches all known sites and finds the best match for this job
        """

        # Assume that jobSplitting has worked,
        # and that every file has the same set of locations
        sites = list(job.getFileLocations())

        tmpSite  = ''
        tmpSlots = -999999
        for loc in sites:
            if not loc in self.slots.keys() or not loc in self.sites.keys():
                self.queue.logger.error('Found job for unknown site %s' %(loc))
                return
            if self.slots[loc] - self.sites[loc] > tmpSlots:
                tmpSlots = self.slots[loc] - self.sites[loc]
                tmpSite  = loc

        return tmpSite