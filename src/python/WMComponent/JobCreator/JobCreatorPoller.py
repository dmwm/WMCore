#!/usr/bin/env python
#pylint: disable-msg=W0613
"""
The JobCreator Poller for the JSM
"""
__all__ = []
__revision__ = "$Id: JobCreatorPoller.py,v 1.7 2010/01/22 22:24:04 mnorman Exp $"
__version__  = "$Revision: 1.7 $"

import threading
import logging
import os
import os.path
import time
#import cProfile, pstats


from WMCore.WorkerThreads.BaseWorkerThread  import BaseWorkerThread
from WMCore.DAOFactory                      import DAOFactory
from WMCore.JobSplitting.SplitterFactory    import SplitterFactory
from WMCore.WMBS.Subscription               import Subscription
from WMCore.WMBS.Job                        import Job

                                            
from WMCore.WMSpec.WMWorkload               import WMWorkload, WMWorkloadHelper
from WMCore.ThreadPool                      import WorkQueue
                                            
                                            
from WMCore.ResourceControl.ResourceControl             import ResourceControl
from WMCore.JobStateMachine.ChangeState                 import ChangeState
from WMCore.WMSpec.Makers.Interface.CreateWorkArea      import CreateWorkArea
from WMCore.ProcessPool.ProcessPool                     import ProcessPool


#from WMCore.WorkQueue.WorkQueue             import WorkQueue





class JobCreatorPoller(BaseWorkerThread):

    """
    Poller that does the work of job creation.
    Polls active subscriptions, asks for more work, and checks with local sites.

    """


    def __init__(self, config):
        """
init jobCreator
        """

        myThread = threading.currentThread()

        #DAO factory for WMBS objects
        self.daoFactory = DAOFactory(package = "WMCore.WMBS", logger = logging, dbinterface = myThread.dbi)

        # WMCore splitter factory for splitting up jobs.
        self.splitterFactory = SplitterFactory()

        #Dictionaries to be filled later
        self.sites         = {}
        self.slots         = {}
        self.workflows     = {}
        self.subscriptions = {}

        #information
        self.config = config


        #Variables
        self.jobCacheDir    = config.JobCreator.jobCacheDir
        self.topOffFactor   = 1.0
        self.defaultJobType = config.JobCreator.defaultJobType

        self.seederDict     = {}
        
        BaseWorkerThread.__init__(self)

        #self.jobMaker = JobMaker(self.config)
        self.createWorkArea  = CreateWorkArea()
        self.resourceControl = ResourceControl()

        configDict = {'jobCacheDir': self.config.JobCreator.jobCacheDir, 
                      'defaultJobType': config.JobCreator.defaultJobType, 
                      'couchURL': self.config.JobStateMachine.couchurl, 
                      'defaultRetries': self.config.JobStateMachine.default_retries,
                      'couchDBName': self.config.JobStateMachine.couchDBName}

        self.processPool = ProcessPool("JobCreator.JobCreatorWorker",
                                       totalSlaves = self.config.JobCreator.workerThreads,
                                       componentDir = self.config.JobCreator.componentDir,
                                       config = self.config, slaveInit = configDict)



        #Testing
        self.timing = {'pollSites': 0, 'pollSubscriptions': 0, 'pollJobs': 0,
                       'askWorkQueue': 0, 'pollSubList': 0, 'pollSubJG': 0, 
                       'pollSubSplit': 0, 'baggage': 0, 'createWorkArea': 0}


        return

    def blank(self):
        """
        Re-initialize a few things

        """

        self.sites         = {}
        self.slots         = {}
        self.workflows     = {}
        self.subscriptions = {}

        self.seederDict    = {}

        return


    def check(self):
        """
        Initial sanity checks on necessary environment factors

        """

        if not os.path.isdir(self.jobCacheDir):
            if not os.path.exists(self.jobCacheDir):
                os.makedirs(self.jobCacheDir)
            else:
                msg = "Assigned a non-existant cache directory %s.  Failing!" % (self.jobCacheDir)
                raise Exception (msg)


    def algorithm(self, parameters):
        """
        Actually runs the code
        """
        logging.debug("Running JSM.JobCreator")
        startTime = time.clock()
        myThread = threading.currentThread()
        #try:
        self.runJobCreator()
        #except Exception, ex:
            #myThread.transaction.rollback()
         #   msg = "Failed to execute JobCreator \n%s" % (ex)
          #  raise Exception(msg)

        #print self.timing
        #print "Job took %f seconds" %(time.clock()-startTime)

        

    def runJobCreator(self):
        """
        Highest level manager for job creation

        """

        myThread = threading.currentThread()


        #This should do three tasks:
        #Poll current subscriptions and create jobs.
        #Poll current jobs and match with sites
        #Ask for new work for sites if necessary

        #print "Starting jobCreator"


        self.blank()
        self.check()

        self.pollJobs()
        self.pollSubscriptions()
        self.askWorkQueue()


        return




    def pollSubscriptions(self):
        """
        Poller for looking in all active subscriptions for jobs that need to be made.

        """
        myThread = threading.currentThread()

        #First, get list of Subscriptions

        subscriptionList = self.daoFactory(classname = "Subscriptions.List")
        subscriptions    = subscriptionList.execute()

        myThread.transaction.commit()


        #Now go through each one looking for jobs
        listOfWork = []
        for subscription in subscriptions:


            #Create a dictionary
            tmpDict = {'subscription': subscription}
            listOfWork.append(tmpDict)
            

        self.processPool.enqueue(listOfWork)

        self.processPool.dequeue(totalItems = len(listOfWork))


        return

    


    def pollJobs(self):
        """
        Poller for checking all active jobs and seeing how many are in each site

        """

        #First, get all subscriptions

        #Then get all locations
        locationList  = self.daoFactory(classname = "Locations.ListSites")
        locations     = locationList.execute()
        
        #Prepare to get all jobs
        jobStates  = ['Executing', 'SubmitFailed', 'JobFailed', 'SubmitCooloff', 'JobCooloff']

        #Find types
        typeFinder = self.daoFactory(classname = "Subscriptions.GetSubTypes")
        types = typeFinder.execute()

        #Get all jobs object
        jobFinder  = self.daoFactory(classname = "Jobs.GetNumberOfJobsPerSite")
        for location in locations:
            self.sites[location] = {}
            for type in types:
                value = int(jobFinder.execute(location = location, states = jobStates, type = type).values()[0])
                self.sites[location][type] = value
                logging.info("There are now %s jobs for site %s" %(self.sites[location], location))
            #Fill sites

        self.pollSites(types)
            
        #You should now have a count of all jobs in self.sites


        #Now we have to make some quick guesses about jobs not yet submitted:
        jobAction = self.daoFactory(classname = "Jobs.GetAllJobs")
        for type in types:
            jobList   = jobAction.execute(state = 'Created', jobType = type)
            for jobID in jobList:
                job = Job(id = jobID)
                job["location"] = self.findSiteForJob(job, type)
                self.sites[job["location"]][type] += 1
        

        return
                
            


    def pollSites(self, types):
        """
        Look for the total number of active sites.
        
        This must be run AFTER pollJobs!

        """

        if not self.config.JobCreator.UpdateFromResourceControl:
            return

        startTime = time.clock()

        #I think this is supposed to be done using siteDB
        #We're getting this from WMCore.Services

        locationAction = self.daoFactory(classname = "Locations.SetJobSlots")

        for location in self.sites.keys():
            self.slots[location] = {}
            slotList = self.resourceControl.getThresholds(siteNames = location)
            slots = 0
            for type in types:
                #Blank the slots
                if not type in self.slots[location].keys():
                    self.slots[location][type] = 0
            for entry in slotList:
                entryName = entry.get('threshold_name', None)
                if entryName.endswith('Threshold'):
                    threshType = entryName.split('Threshold')[0]  #Grab the first part
                    self.slots[location][threshType] = entry.get('threshold_value', 0)
            #locationAction.execute(location, slots)

        self.timing['pollSites'] += (time.clock() - startTime)
        return


    def askWorkQueue(self):
        """
        This probably does more then it should: it checks every site,
        and then decides whether they're full or not

        """

        if not self.config.JobCreator.useWorkQueue:
            return

        #startTime = time.clock()

        workQueueDict = {}

        for location in self.sites.keys():
            #This is the number of free slots - the number of Created but not Exectuing jobs
            freeSlots = (self.slots[location]['Processing'] * self.topOffFactor) - self.sites[location]['Processing']

            #I need freeSlots jobs on site location
            logging.info('I need %s jobs on site %s' %(freeSlots, location))

            if freeSlots < 0:
                freeSlots = 0
            workQueueDict[location] = freeSlots


        #workQueue.getWork(workQueueDict)

        #self.timing['askWorkQueue'] += (time.clock() - startTime)
        return

        



    #Assistance functions


    def retrieveWMSpec(self, subscription):
        """
        _retrieveWMSpec_

        Given a subscription, this function loads the WMSpec
        associated with that workload
        """
        workflow = subscription['workflow']
        wmWorkloadURL = workflow.spec

        if not os.path.isfile(wmWorkloadURL):
            return None

        wmWorkload = WMWorkloadHelper(WMWorkload("workload"))
        wmWorkload.load(wmWorkloadURL)  

        return wmWorkload



    def retrieveJobSplitParams(self, wmWorkload):
        """
        _retrieveJobSplitParams_

        Retrieve job splitting parameters from the workflow.  The way this is
        setup currently sucks, we have to know all the job splitting parameters
        up front.  The following are currently supported:
          files_per_job
          min_merge_size
          max_merge_size
          max_merge_events
        """


        #This function has to find the WMSpec, and get the parameters from the spec
        #I don't know where the spec is, but I'll have to find it.
        #I don't want to save it in each workflow area, but I may have to

        foundParams = True

        if not wmWorkload:
            foundParams = False
        elif not type(wmWorkload.data.split.splitParams) == dict:
            foundParams = False


        if not foundParams:
            return {"files_per_job": 5}
        else:
            return wmWorkload.data.split.splitParams


    def createJobGroup(self, wmbsJobGroup, jobGroupConfig, wmbsSubscription, wmWorkload):
        """
        Pass this on to the jobCreator, which actually does the work
        
        """

        myThread = threading.currentThread()

        myThread.transaction.begin()

        #logging.info('I am in createJobGroup for jobGroup %i' %(wmbsJobGroup.id))

        changeState = ChangeState(self.config)

        #Here things get interesting.
        #We assume that this follows the basic scheme for the jobGroups, that each jobGroup contains
        #files with only one set of sites.
        #Using this we can determine the number of free slots for each job.


        for job in wmbsJobGroup.jobs:
            #logging.info('Saving jobs for %i in jobGroup %i' %(wmbsSubscription['id'], wmbsJobGroup.id))
            job["location"] = self.findSiteForJob(job)
            self.sites[job["location"]] += 1



        #Create the job
        changeState.propagate(wmbsJobGroup.jobs, 'created', 'new')
        myThread.transaction.commit()

        logging.info("JobCreator has changed jobs to Created for jobGroup %i and is ending" %(wmbsJobGroup.id))


        return

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


    def terminate(self, params):
        """
        _terminate_
        
        Kill the code after one final pass when called by the master thread.
        """
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)





