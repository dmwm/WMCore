#!/usr/bin/env python
#pylint: disable-msg=W0613, W6501
"""
The JobCreator Poller for the JSM
"""
__all__ = []
__revision__ = "$Id: JobCreatorPoller.py,v 1.14 2010/03/22 16:21:44 sryu Exp $"
__version__  = "$Revision: 1.14 $"

import threading
import logging
import os
import os.path
import traceback
import time
#import cProfile, pstats


from WMCore.WorkerThreads.BaseWorkerThread  import BaseWorkerThread
from WMCore.DAOFactory                      import DAOFactory
from WMCore.JobSplitting.SplitterFactory    import SplitterFactory
from WMCore.WMBS.Job                        import Job

                                            
from WMCore.WMSpec.WMWorkload               import WMWorkload, WMWorkloadHelper
#from WMCore.ThreadPool                      import WorkQueue
                                            
                                            
from WMCore.ResourceControl.ResourceControl             import ResourceControl
from WMCore.JobStateMachine.ChangeState                 import ChangeState
from WMCore.WMSpec.Makers.Interface.CreateWorkArea      import CreateWorkArea
from WMCore.ProcessPool.ProcessPool                     import ProcessPool
from WMCore.WorkQueue.WorkQueue                         import localQueue




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
        self.daoFactory = DAOFactory(package = "WMCore.WMBS", \
                                     logger = logging, dbinterface = myThread.dbi)

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

        if self.config.JobCreator.useWorkQueue:
            self.workQueue = localQueue(**self.config.JobCreator.WorkQueueParam)
        
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
                msg = "Assigned a non-existant cache directory %s.  Failing!" \
                      % (self.jobCacheDir)
                raise Exception (msg)


    def algorithm(self, parameters = None):
        """
        Actually runs the code
        """
        logging.debug("Running JSM.JobCreator")
        try:
            self.runJobCreator()
        except Exception, ex:
            #myThread.transaction.rollback()
            msg = "Failed to execute JobCreator \n%s\n" % (ex)
            msg += str(traceback.format_exc())
            msg += "\n\n"
            raise Exception(msg)

        #print self.timing
        #print "Job took %f seconds" %(time.clock()-startTime)

        

    def runJobCreator(self):
        """
        Highest level manager for job creation

        """

        #This should do three tasks:
        #Poll current subscriptions and create jobs.
        #Poll current jobs and match with sites
        #Ask for new work for sites if necessary

        self.blank()
        self.check()

        self.pollJobs()
        self.pollSubscriptions()
        if self.config.JobCreator.useWorkQueue:
            self.askWorkQueue()
        return




    def pollSubscriptions(self):
        """
        Poller for looking in all active subscriptions for jobs that need to be made.

        """
        myThread = threading.currentThread()

        #First, get list of Subscriptions

        subscriptionList = self.daoFactory(classname = "Subscriptions.ListIncomplete")
        subscriptions    = subscriptionList.execute()

        myThread.transaction.commit()


        #Now go through each one looking for jobs
        listOfWork = []
        for subscription in subscriptions:


            #Create a dictionary
            tmpDict = {'subscription': subscription}
            listOfWork.append(tmpDict)
            
        if listOfWork != []:
            # Only enqueue if we have work to do!
            self.processPool.enqueue(listOfWork)
            
            self.processPool.dequeue(totalItems = len(listOfWork))


        return

    

    def pollJobs(self):
        """
        Survey sites for number of open slots; number of running jobs

        """

        if not self.config.JobCreator.UpdateFromResourceControl:
            return

        siteRCDict = self.resourceControl.listThresholdsForCreate()

        # This should be two tiered: 1st location, 2nd number of slots

        for site in siteRCDict.keys():
            self.sites[site] = siteRCDict[site]['running_jobs']
            self.slots[site] = siteRCDict[site]['total_slots']
            logging.info("There are now %s jobs for site %s" \
                         %(self.sites[site], site))


        # Now we have to make some quick guesses about jobs not yet submitted:
        jobAction = self.daoFactory(classname = "Jobs.GetAllJobs")
        jobList   = jobAction.execute(state = 'Created')
        for jobID in jobList:
            job = Job(id = jobID)
            job["location"] = self.findSiteForJob(job)
            self.sites[job["location"]] += 1
        


    def askWorkQueue(self):
        """
        This probably does more then it should: it checks every site,
        and then decides whether they're full or not

        """

        startTime = time.clock()

        workQueueDict = {}

        for location in self.sites.keys():
            #This is the number of free slots
            # - the number of Created but not Exectuing jobs
            freeSlots = (self.slots[location] * self.topOffFactor) \
                        - self.sites[location]

            #I need freeSlots jobs on site location
            logging.info('I need %s jobs on site %s' %(freeSlots, location))

            if freeSlots < 0:
                freeSlots = 0
            workQueueDict[location] = freeSlots

        self.workQueue.getWork(workQueueDict)

        self.timing['askWorkQueue'] += (time.clock() - startTime)
        return

        



    #Assistance functions



    def createJobGroup(self, wmbsJobGroup, jobGroupConfig, \
                       wmbsSubscription, wmWorkload):
        """
        Pass this on to the jobCreator, which actually does the work
        
        """

        myThread = threading.currentThread()

        myThread.transaction.begin()

        changeState = ChangeState(self.config)

        #Here things get interesting.
        #We assume that this follows the basic scheme for the jobGroups,
        # that each jobGroup contains
        #files with only one set of sites.
        #Using this we can determine the number of free slots for each job.


        for job in wmbsJobGroup.jobs:
            job["location"] = self.findSiteForJob(job)
            self.sites[job["location"]] += 1



        #Create the job
        changeState.propagate(wmbsJobGroup.jobs, 'created', 'new')
        myThread.transaction.commit()

        logging.info("JobCreator has changed jobs to Created for jobGroup %i and is ending" %(wmbsJobGroup.id))


        return

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
                logging.error('Found job for unknown site %s' %(loc))
                return
            if self.slots[loc] - self.sites[loc] > tmpSlots:
                tmpSlots = self.slots[loc] - self.sites[loc]
                tmpSite  = loc

        return tmpSite


    def terminate(self, params):
        """
        _terminate_
        
        Kill the code after one final pass when called by the master thread.
        """
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)





