#!/usr/bin/env python
#pylint: disable-msg=W0613
"""
The JobCreator Poller for the JSM
"""
__all__ = []
__revision__ = "$Id: JobCreatorPoller.py,v 1.4 2009/10/05 20:08:39 mnorman Exp $"
__version__ = "$Revision: 1.4 $"
__author__ = "mnorman@fnal.gov"

import threading
import logging
import re
import os
import os.path
import time
import random
import inspect
#import cProfile, pstats

import pickle


from WMCore.WorkerThreads.BaseWorkerThread  import BaseWorkerThread

from WMCore.WMFactory                       import WMFactory
from WMCore.DAOFactory                      import DAOFactory
from WMCore.JobSplitting.SplitterFactory    import SplitterFactory
                                            
from WMCore.WMBS.Subscription               import Subscription
from WMCore.WMBS.Fileset                    import Fileset
from WMCore.WMBS.Workflow                   import Workflow
                                            
from WMCore.WMSpec.WMWorkload               import WMWorkload, WMWorkloadHelper
from WMCore.WMSpec.WMTask                   import WMTask, WMTaskHelper
                                            
                                            
from WMComponent.JobCreator.JobCreatorSiteDBInterface   import JobCreatorSiteDBInterface as JCSDBInterface

from WMCore.WMSpec.Seeders.SeederManager                import SeederManager
from WMCore.ResourceControl.ResourceControl             import ResourceControl
from WMCore.JobStateMachine.ChangeState                 import ChangeState

from WMCore.WMSpec.Makers.JobMaker                      import JobMaker
from WMCore.WMSpec.Makers.Interface.CreateWorkArea      import CreateWorkArea

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


        #Testing
        self.timing = {'pollSites': 0, 'pollSubscriptions': 0, 'pollJobs': 0, 'askWorkQueue': 0, 'pollSubList': 0, 'pollSubJG': 0, \
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
        self.manager       = None

        return


    def check(self):
        """
        Initial sanity checks on necessary environment factors

        """

        if not os.path.isdir(self.jobCacheDir):
            msg = "Assigned a non-existant cache directory %s.  Failing!" %(self.jobCacheDir)
            raise Exception (msg)


    def algorithm(self, parameters):
        """
        Actually runs the code
        """
        logging.debug("Running JSM.JobCreator")
        #self.jobMaker.prepareToStart()
        startTime = time.clock()
        myThread = threading.currentThread()
        try:
            self.runJobCreator()
        except Exception, ex:
            myThread.transaction.rollback()
            msg = "Failed to execute JobCreator \n%s" %(ex)
            raise Exception(msg)

        print self.timing
        print "Job took %f seconds" %(time.clock()-startTime)

        

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

        #print "pollJobs"
        self.pollJobs()
        #print "pollSites"
        self.pollSites()
        #print "pollSubs"
        self.pollSubscriptions()
        #print "askWorkQueue"
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
        for subscription in subscriptions:



            myThread.transaction.begin()

            wmbsSubscription = Subscription(id = subscription)
            wmbsSubscription.load()
            #if not len(wmbsSubscription.availableFiles()) > 0:
                #continue
            #I need this later
            wmbsSubscription["workflow"].load()

            #print "Have %i files" %(len(wmbsSubscription.availableFiles()))

            #My hope is that the job factory is smart enough only to split un-split jobs
            jsStartTime = time.clock()
            wmbsJobFactory = self.splitterFactory(package = "WMCore.WMBS", subscription = wmbsSubscription)

            wmWorkload  = self.retrieveWMSpec(wmbsSubscription)
            splitParams = self.retrieveJobSplitParams(wmWorkload)

            wmbsJobGroups = wmbsJobFactory(**splitParams)

            self.timing['pollSubSplit'] += (time.clock() - jsStartTime)

            myThread.transaction.commit()

            jobGroupConfig = {}

            cwaStartTime = time.clock()
            for wmbsJobGroup in wmbsJobGroups:
                self.createJobGroup(wmbsJobGroup, jobGroupConfig, wmbsSubscription, wmWorkload)
                #Create a directory
                self.createWorkArea.processJobs(jobGroupID = wmbsJobGroup.exists(), startDir = self.jobCacheDir)

            self.timing['createWorkArea'] += (time.clock() - cwaStartTime)

            bagStartTime = time.clock()
            for wmbsJobGroup in wmbsJobGroups:
                for job in wmbsJobGroup.jobs:
                    #Now, if we had the seeder do something, we save it
                    baggage = job.getBaggage()
                    #If there's something there, do something with it.
                    if baggage:
                        cacheDir = job.getCache()
                        #print "About to dump baggage"
                        output = open(os.path.join(cacheDir, 'baggage.pcl'),'w')
                        pickle.dump(baggage, output)
                        output.close()
            self.timing['baggage'] += (time.clock() - bagStartTime)

        return


    


    def pollJobs(self):
        """
        Poller for checking all active jobs and seeing how many are in each site

        """

        myThread = threading.currentThread()

        #First, get all subscriptions

        #Then get all locations
        locationList  = self.daoFactory(classname = "Locations.ListSites")
        locations     = locationList.execute()
        
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
                
            


    def pollSites(self):
        """
        Look for the total number of active sites.
        
        This must be run AFTER pollJobs!

        """

        if not self.config.JobCreator.UpdateFromSiteDB:
            return

        startTime = time.clock()

        #I think this is supposed to be done using siteDB
        #We're getting this from WMCore.Services

        myThread = threading.currentThread()

        siteDBconfig = {}

        #Don't uncomment this unless you want to overwrite the siteDB service logger
        #siteDBconfig['logger'] =  logging

        siteDB = JCSDBInterface(siteDBconfig)

        locationAction = self.daoFactory(classname = "Locations.SetJobSlots")

        for location in self.sites.keys():
            #slots = siteDB.getPledgedSlots(location)
            slotList = self.resourceControl.getThresholds(siteNames = location)
            slots = 0
            for entry in slotList:
                if entry.get('threshold_name', None) == '%sThreshold' %(self.defaultJobType):
                    slots = entry.get('threshold_value', 0)
                    break
            locationAction.execute(location, slots)
            self.slots[location] = slots

        self.timing['pollSites'] += (time.clock() - startTime)
        return


    def askWorkQueue(self):
        """
        This probably does more then it should: it checks every site, and then decides whether they're full or not

        """

        #logging.info('Off to see the work queue')
        
        startTime = time.clock()

        workQueueDict = {}

        for location in self.sites.keys():
            #This is the number of free slots - the number of Created but not Exectuing jobs
            freeSlots = (self.slots[location] * self.topOffFactor) - self.sites[location]

            #I need freeSlots jobs on site location
            logging.info('I need %s jobs on site%s' %(freeSlots, location))

            if freeSlots < 0:
                freeSlots = 0
            workQueueDict[location] = freeSlots


        #workQueue.getWork(workQueueDict)

        self.timing['askWorkQueue'] += (time.clock() - startTime)
        return

        



    #Assistance functions


    def retrieveWMSpec(self, subscription):
        """
        _retrieveWMSpec_

        Given a subscription, this function loads the WMSpec associated with that workload
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

        #logging.info("Looking for Split Params")

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

        workflow = wmbsSubscription["workflow"]
        if not workflow.task or not wmWorkload:
            wmTask = None
        else:
            wmTask = wmWorkload.getTask(workflow.task)
            if hasattr(wmTask.data, 'seeders'):
                manager = SeederManager(wmTask)
                manager(wmbsJobGroup)
            else:
                wmTask = None

        taskDir = False


        for job in wmbsJobGroup.jobs:
            #logging.info('Saving jobs for %i in jobGroup %i' %(wmbsSubscription['id'], wmbsJobGroup.id))
            job["location"] = self.findSiteForJob(job)
            self.sites[job["location"]] += 1



        #Create the job
        changeState.propagate(wmbsJobGroup.jobs, 'created', 'new')
        myThread.transaction.commit()
        #logging.info(len(wmbsJobGroup.jobs))
        #logging.info(myThread.dbi.processData("SELECT * FROM wmbs_job")[0].fetchall())

        logging.info("JobCreator has changed jobs to Created for jobGroup %i and is ending" %(wmbsJobGroup.id))


        return

    def findSiteForJob(self, job):
        """
        _findSiteForJob_

        This searches all known sites and finds the best match for this job
        """

        myThread = threading.currentThread()

        #Assume that jobSplitting has worked, and that every file has the same set of locations
        sites = list(job.getFiles()[0]['locations'])

        tmpSite  = ''
        tmpSlots = 0
        for loc in sites:
            if not loc in self.slots.keys() or not loc in self.sites.keys():
                logging.error('Found job for unknown site %s' %(loc))
                logging.error('ABORT: Am not processing jobGroup %i' %(wmbsJobGroup.id))
                return
            if self.slots[loc] - self.sites[loc] > tmpSlots:
                tmpSlots = self.slots[loc] - self.sites[loc]
                tmpSite  = loc

        return tmpSite


    def terminate(self,params):
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)


