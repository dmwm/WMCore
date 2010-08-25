#!/usr/bin/env python
#pylint: disable-msg=W0102
#W0102: We want to pass blank lists by default for the whitelist and the blacklist

"""
Creates jobs for new subscriptions

"""

__revision__ = "$Id: JobSubmitterPoller.py,v 1.7 2010/01/22 22:09:19 mnorman Exp $"
__version__ = "$Revision: 1.7 $"


#This job currently depends on the following config variables in JobSubmitter:
# pluginName
# pluginDir

import logging
import threading
import time
import os.path
import string
#import common

#WMBS objects
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Job          import Job
from WMCore.DAOFactory        import DAOFactory

from WMCore.WMSpec.WMWorkload                 import WMWorkload, WMWorkloadHelper
from WMCore.JobStateMachine.ChangeState       import ChangeState
from WMCore.WorkerThreads.BaseWorkerThread    import BaseWorkerThread
from WMCore.ProcessPool.ProcessPool           import ProcessPool
from WMCore.ResourceControl.ResourceControl   import ResourceControl



class JobSubmitterPoller(BaseWorkerThread):
    """
    Handles job submission

    """

    def __init__(self, config):

        myThread = threading.currentThread()

        #DAO factory for WMBS objects
        self.daoFactory = DAOFactory(package = "WMCore.WMBS", logger = logging, dbinterface = myThread.dbi)

        #Dictionary definitions
        self.slots     = {}
        self.sites     = {}
        self.locations = {}

        #Set config objects
        self.database = config.CoreDatabase.connectUrl

        (connectDialect, junk) = config.CoreDatabase.connectUrl.split(":", 1)
        if connectDialect.lower() == "mysql":
            self.dialect = "MySQL"
        elif connectDialect.lower() == "oracle":
            self.dialect = "Oracle"
        elif connectDialect.lower() == "sqlite":
            self.dialect = "SQLite"
        
        self.session = None
        self.schedulerConfig = {}
        self.config = config
        self.types = []

        #Libraries
        self.resourceControl = ResourceControl()

        BaseWorkerThread.__init__(self)

        configDict = {'submitDir': self.config.JobSubmitter.submitDir, 'submitNode': self.config.JobSubmitter.submitNode,
                      'submitScript': self.config.JobSubmitter.submitScript}
        if hasattr(self.config.JobSubmitter, 'inputFile'):
            configDict['inputFile'] = self.config.JobSubmitter.inputFile

        workerName = "%s.%s" % (self.config.JobSubmitter.pluginDir, self.config.JobSubmitter.pluginName)

        self.processPool = ProcessPool(workerName,
                                       totalSlaves = self.config.JobSubmitter.workerThreads,
                                       componentDir = self.config.JobSubmitter.componentDir,
                                       config = self.config, slaveInit = configDict)

        return

    def algorithm(self, parameters):
        """
        Actually runs the code
        """
        logging.debug("Running JSM.JobSubmitter")
        myThread = threading.currentThread()
        try:
            startTime = time.clock()
            self.runSubmitter()
            stopTime = time.clock()
            logging.debug("Running jobSubmitter took %f seconds" %(stopTime - startTime))
            #print "Runtime for JobSubmitter %f" %(stopTime - startTime)
            #print self.timing
        except:
            #myThread.transaction.rollback()
            raise


    def runSubmitter(self):
        """
        _runSubmitter_

        Keeps track of, and does, everything
        """

        #self.configure()
        self.setLocations()
        self.pollJobs()
        jobList = self.getJobs()
        #jobList = self.setJobLocations(jobList)
        jobList = self.grabTask(jobList)
        self.submitJobs(jobList)


        idList = []
        for job in jobList:
            idList.append({'jobid': job['id'], 'location': job['location']})
        setLocationAction = self.daoFactory(classname = "Jobs.SetLocation")
        setLocationAction.execute(bulkList = idList)

        return


    def setLocations(self):
        self.locations = {}

        #Then get all locations
        locationList            = self.daoFactory(classname = "Locations.List")
        #locationSlots           = self.daoFactory(classname = "Locations.GetJobSlots")

        #Find types
        typeFinder = self.daoFactory(classname = "Subscriptions.GetSubTypes")
        self.types = typeFinder.execute()

        locations = locationList.execute()

        for loc in locations:
            location = loc[1] #It's just a format issue
            self.slots[location] = {}
            slotList = self.resourceControl.getThresholds(siteNames = location)
            slots = 0
            for type in self.types:
                #Blank the slots
                if not type in self.slots[location].keys():
                    self.slots[location][type] = 0
            for entry in slotList:
                entryName = entry.get('threshold_name', None)
                if entryName.endswith('Threshold'):
                    threshType = entryName.split('Threshold')[0]  #Grab the first part
                    self.slots[location][threshType] = entry.get('threshold_value', 0)
        return

    def getJobs(self):
        """
        _getJobs_

        This uses WMBS to extract a list of jobs in the 'Created' state
        """
        newList = []

        getJobs = self.daoFactory(classname = "Jobs.GetAllJobs")
        for type in self.types:
            jobList   = getJobs.execute(state = 'Created', jobType = type)
            for jobID in jobList:
                job = Job(id = jobID)
                job.load()
                job["location"] = self.findSiteForJob(job, type)
                self.sites[job["location"]][type] += 1
                newList.append(job)

        return newList

    def findSiteForJob(self, job, type):
        """
        _findSiteForJob_

        This searches all known sites and finds the best match for this job
        """

        myThread = threading.currentThread()

        #Assume that jobSplitting has worked, and that every file has the same set of locations
        sites = list(job.getFileLocations())

        tmpSite  = ''
        tmpSlots = -999999
        for loc in sites:
            if not loc in self.slots.keys() or not loc in self.sites.keys():
                logging.error('Found job for unknown site %s' %(loc))
                logging.error(self.slots)
                logging.error(self.sites)
                return
            if self.slots[loc][type] - self.sites[loc][type] > tmpSlots:
                tmpSlots = self.slots[loc][type] - self.sites[loc][type]
                tmpSite  = loc

        return tmpSite


    def pollJobs(self):
        """
        Poller for checking all active jobs and seeing how many are in each site

        """

        #Then get all locations
        locationList  = self.daoFactory(classname = "Locations.ListSites")
        locations     = locationList.execute()
        
        logging.info(locations)

        #Prepare to get all jobs
        jobStates  = ['Created', 'Executing', 'SubmitFailed', 'JobFailed', 'SubmitCooloff', 'JobCooloff']

        #Get all jobs object
        jobFinder  = self.daoFactory(classname = "Jobs.GetNumberOfJobsPerSite")
        for location in locations:
            self.sites[location] = {}
            for type in self.types:
                value = int(jobFinder.execute(location = location, states = jobStates, type = type).values()[0])
                self.sites[location][type] = value
                logging.info("There are now %s jobs for site %s" %(self.sites[location], location))
            
        #You should now have a count of all jobs in self.sites

        return


    def submitJobs(self, jobList):
        """
        _submitJobs_
        
        This runs over the list of jobs and submits them all
        """

        myThread = threading.currentThread()

        changeState = ChangeState(self.config)

        listOfJobs = jobList[:]
        count = 0

        while len(listOfJobs) > self.config.JobSubmitter.jobsPerWorker:
            listForSub = listOfJobs[:self.config.JobSubmitter.jobsPerWorker]
            listOfJobs = listOfJobs[self.config.JobSubmitter.jobsPerWorker:]
            self.processPool.enqueue([{'jobs': listForSub}])
            count += 1
        if len(listOfJobs) > 0:
            self.processPool.enqueue([{'jobs': listOfJobs}])
            count += 1

        #result = self.processPool.dequeue(len(jobList))
        result = []
        #for i in range(0, count):
        result = self.processPool.dequeue(count)

        #This will return a list of dictionaries of job ids

        successList = []
        failList    = []

        successCompilation = []
        for entry in result:
            if 'Success' in entry.keys():
                successCompilation.extend(entry['Success'])

        for job in jobList:
            if job['id'] in successCompilation:
                successList.append(job)
            else:
                failList.append(job)

        #Pass the successful jobs, and fail the bad ones
        myThread.transaction.begin()
        changeState.propagate(successList, 'executing',    'created')
        changeState.propagate(failList,    'submitfailed', 'created')

        myThread.transaction.commit()

        return


    def grabTask(self, jobList):
        """
        _grabTask_

        Grabs the task, sandbox, etc for each job by using the WMBS DAO object
        """

        myThread = threading.currentThread()

        taskFinder = self.daoFactory(classname="Jobs.GetTask")

        #Assemble list
        jobIDs = []
        for job in jobList:
            jobIDs.append(job['id'])

        tasks = taskFinder.execute(jobID = jobIDs)

        taskDict = {}
        for job in jobList:
            #Now it gets interesting
            #Load the WMTask and grab the info that you need
            jobID = job['id']
            workloadName = tasks[jobID].split('/')[0]
            taskName = tasks[jobID].split('/')[1:]
            if type(taskName) == list:
                taskName = string.join(taskName, '/')
            #If we haven't picked this up before, pick it up now
            if not workloadName in taskDict.keys():
                #We know the format that the path is in
                workloadPath = os.path.join(self.config.WMAgent.WMSpecDirectory, '%s.pcl' %(workloadName))
                wmWorkload = WMWorkloadHelper(WMWorkload("workload"))
                if not os.path.isfile(workloadPath):
                    workloadPath = os.path.join(self.config.WMAgent.WMSpecDirectory, '%s.pckl' %(workloadName))
                    if not os.path.isfile(workloadPath):
                        logging.error("Could not find WMSpec file %s in path %s for job %i" %(workloadName, workloadPath, jobID))
                        continue
                wmWorkload.load(os.path.join(self.config.WMAgent.WMSpecDirectory, '%s.pcl' %(workloadName)))
                taskDict[workloadName] = wmWorkload
                

            task = taskDict[workloadName].getTask(taskName)
            if not hasattr(task.data.input, 'sandbox'):
                logging.error("Job %i has no sandbox!" %(jobID))
                continue
            job['sandbox'] = task.data.input.sandbox
            

        return jobList

    def terminate(self, params):
        """
        _terminate_
        
        Terminate code after final pass.
        """
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)


        


        




