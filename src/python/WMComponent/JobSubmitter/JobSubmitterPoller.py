#!/usr/bin/env python

"""
Creates jobs for new subscriptions

"""

__revision__ = "$Id: JobSubmitterPoller.py,v 1.3 2009/11/06 19:29:49 mnorman Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "anzar@fnal.gov"


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
from WMCore.WMBS.JobGroup     import JobGroup
from WMCore.WMBS.Job          import Job as Job
from WMCore.WMBS.Workflow     import Workflow
from WMCore.DAOFactory        import DAOFactory
from WMCore.WMFactory         import WMFactory

from WMCore.WMSpec.WMWorkload                 import WMWorkload, WMWorkloadHelper
from WMCore.WMSpec.WMTask                     import WMTask, WMTaskHelper


from WMCore.JobStateMachine.ChangeState import ChangeState


from WMCore.WorkerThreads.BaseWorkerThread  import BaseWorkerThread


from WMCore.ProcessPool.ProcessPool                     import ProcessPool




class JobSubmitterPoller(BaseWorkerThread):
    """
    Handles job submission

    """

    def __init__(self, config):

        myThread = threading.currentThread()

        #DAO factory for WMBS objects
        self.daoFactory = DAOFactory(package = "WMCore.WMBS", logger = logging, dbinterface = myThread.dbi)

        #Dictionary definitions
        self.slots = {}
        self.sites = {}

        #Set config objects
        self.database = config.CoreDatabase.connectUrl
        self.dialect  = config.CoreDatabase.dialect

        self.session = None
        self.schedulerConfig = {}
        self.cfg_params = None
        self.config = config

        BaseWorkerThread.__init__(self)

        configDict = {'submitDir': self.config.JobSubmitter.submitDir, 'submitNode': self.config.JobSubmitter.submitNode,
                      'submitScript': self.config.JobSubmitter.submitScript}

        workerName = "%s.%s" %(self.config.JobSubmitter.pluginDir, self.config.JobSubmitter.pluginName)

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
            myThread.transaction.rollback()
            raise

    def runSubmitter(self):
        """
        _runSubmitter_

        Keeps track of, and does, everything
        """

        myThread = threading.currentThread()

        #self.configure()
        self.setLocations()
        self.pollJobs()
        jobList = self.getJobs()
        jobList = self.setJobLocations(jobList)
        jobList = self.grabTask(jobList)
        self.submitJobs(jobList)


        idList = []
        for job in jobList:
            idList.append({'jobid': job['id'], 'location': job['location']})
        SetLocationAction = self.daoFactory(classname = "Jobs.SetLocation")
        SetLocationAction.execute(bulkList = idList)
                          

        return


    def setLocations(self):
        self.locations = {}

        #Then get all locations
        locationList            = self.daoFactory(classname = "Locations.List")
        locationSlots           = self.daoFactory(classname = "Locations.GetJobSlots")

        locations = locationList.execute()

        for loc in locations:
            location = loc[1]  #We need this because locations are returned as a list
            value = locationSlots.execute(siteName = location)
            self.slots[location] = value

        return

    def getJobs(self):
        """
        _getJobs_

        This uses WMBS to extract a list of jobs in the 'Created' state
        """

        getJobs = self.daoFactory(classname = "Jobs.GetAllJobs")
        jobList = getJobs.execute(state = "Created")

        return jobList

    def setJobLocations(self, jobList, whiteList = [], blackList = []):
        """
        _setJobLocations

        Set the locations for each job based on current knowledge
        """

        newList = []

        for jid in jobList:
            job = Job(id = jid)
            location = self.findSiteForJob(job)
            job["location"] = location
            newList.append(job)

        return newList

    def findSiteForJob(self, job):
        """
        _findSiteForJob_

        This searches all known sites and finds the best match for this job
        """

        myThread = threading.currentThread()

        #Assume that jobSplitting has worked, and that every file has the same set of locations
        sites = list(job.getFiles()[0]['locations'])

        tmpSite  = ''
        tmpSlots = -1
        for loc in sites:
            if not loc in self.slots.keys() or not loc in self.sites.keys():
                logging.error('Found job for unknown site %s' %(loc))
                logging.error('ABORT: Am not processing jobGroup %i' %(wmbsJobGroup.id))
                return
            if self.slots[loc] - self.sites[loc] > tmpSlots:
                tmpSlots = self.slots[loc] - self.sites[loc]
                tmpSite  = loc

        return tmpSite


    def pollJobs(self):
        """
        Poller for checking all active jobs and seeing how many are in each site

        """

        myThread = threading.currentThread()

        #Then get all locations
        locationList  = self.daoFactory(classname = "Locations.ListSites")
        locations     = locationList.execute()
        
        logging.info(locations)

        #Prepare to get all jobs
        jobStates  = ['Created', 'Executing', 'SubmitFailed', 'JobFailed', 'SubmitCooloff', 'JobCooloff']

        #Get all jobs object
        jobFinder  = self.daoFactory(classname = "Jobs.GetNumberOfJobsPerSite")
        for location in locations:
            value = int(jobFinder.execute(location = location, states = jobStates).values()[0])
            self.sites[location] = value
            logging.info("There are now %i jobs for site %s" %(self.sites[location], location))
            
        #You should now have a count of all jobs in self.sites

        return


    def submitJobs(self, jobList, localConfig = {}, subscription = None):
        """
        _submitJobs_
        
        This runs over the list of jobs and submits them all
        """

        myThread = threading.currentThread()

        changeState = ChangeState(self.config)

        listOfJobs = jobList[:]
        count = 0

        while len(listOfJobs) > self.config.JobSubmitter.jobsPerWorker:
            listForSub = [listOfJobs[:self.config.JobSubmitter.jobsPerWorker]]
            listOfJobs = listOfJobs[self.config.JobSubmitter.jobsPerWorker:]
            self.processPool.enqueue(listForSub)
            count += 1
        if len(listOfJobs) > 0:
            self.processPool.enqueue([listOfJobs])
            count += 1

        #result = self.processPool.dequeue(len(jobList))
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
        changeState.propagate(failList,    'SubmitFailed', 'Created')

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

    def terminate(self,params):
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)


        


        




