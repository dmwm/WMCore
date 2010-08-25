#!/usr/bin/env python
#pylint: disable-msg=W0613
"""
The JobCreator Poller for the JSM
"""
__all__ = []
__revision__ = "$Id: JobCreatorPoller.py,v 1.2 2009/08/10 14:35:24 mnorman Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "mnorman@fnal.gov"

import threading
import logging
import re
import os
import time
import random
import inspect


from WMCore.WorkerThreads.BaseWorkerThread  import BaseWorkerThread

from WMCore.WMFactory                       import WMFactory
from WMCore.DAOFactory                      import DAOFactory
from WMCore.JobSplitting.SplitterFactory    import SplitterFactory
                                            
from WMCore.WMBS.Subscription               import Subscription
from WMCore.WMBS.Fileset                    import Fileset
from WMCore.WMBS.Workflow                   import Workflow
                                            
from WMCore.WMSpec.WMWorkload               import WMWorkload, WMWorkloadHelper
from WMCore.WMSpec.WMTask                   import WMTask, WMTaskHelper
                                            
                                            
from WMComponent.JobCreator.JobCreatorSiteDBInterface import JobCreatorSiteDBInterface as JCSDBInterface

from WMComponent.JobSubmitter.JobSubmitter    import JobSubmitter

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
        self.jobCacheDir    = os.getcwd() + '/tmp'
        self.topOffFactor   = 1.0
        
        BaseWorkerThread.__init__(self)


        return

    def blank(self):
        """
        Re-initialize a few things

        """

        self.sites         = {}
        self.slots         = {}
        self.workflows     = {}
        self.subscriptions = {}

        return


    def algorithm(self, parameters):
        """
        Actually runs the code
        """
        logging.debug("Running JSM.JobCreator")
        myThread = threading.currentThread()
        try:
            self.runJobCreator()
        except:
            myThread.transaction.rollback()
            raise


    def runJobCreator(self):
        """
        Highest level manager for job creation

        """

        myThread = threading.currentThread()


        #This should do three tasks:
        #Poll current subscriptions and create jobs.
        #Poll current jobs and match with sites
        #Ask for new work for sites if necessary






        self.blank()

        #logging.info(myThread.dbi.processData("SELECT id FROM wmbs_jobgroup", {})[0].fetchall())

        #logging.info(myThread.dbi.processData("SELECT id FROM wmbs_jobgroup", {})[0].fetchall())
        self.pollJobs()
        self.pollSites()
        self.pollSubscriptions()
        #logging.info(myThread.dbi.processData("SELECT id FROM wmbs_jobgroup", {})[0].fetchall())
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
            wmbsSubscription.loadData()

            #My hope is that the job factory is smart enough only to split un-split jobs
            wmbsJobFactory = self.splitterFactory(package = "WMCore.WMBS", subscription = wmbsSubscription)

            splitParams = self.retrieveJobSplitParams(wmbsSubscription)

            wmbsJobGroups = wmbsJobFactory(**splitParams)
            
            myThread.transaction.commit()

           # logging.info('The jobs groups should be created')
            #logging.info(myThread.dbi.processData("SELECT id FROM wmbs_jobgroup", {})[0].fetchall())

            jobGroupConfig = {}

            for wmbsJobGroup in wmbsJobGroups:
                self.createJobGroup(wmbsJobGroup, jobGroupConfig, wmbsSubscription)



        return


    


    def pollJobs(self):
        """
        Poller for checking all active jobs and seeing how many are in each site

        """

        myThread = threading.currentThread()

        #First, get all subscriptions
        subscriptionList = self.daoFactory(classname = "Subscriptions.List")
        subscriptions    = subscriptionList.execute()

        #Then get all locations
        locationList  = self.daoFactory(classname = "Locations.ListSites")
        locations     = locationList.execute()
        
        logging.info(locations)

        #Prepare to get all jobs
        jobsList   = self.daoFactory(classname = "Subscriptions.Jobs")
        jobStates  = ['Created', 'Executing', 'SubmitFailed', 'JobFailed', 'SubmitCooloff', 'JobCooloff']

        #logging.info('Created factories')

        for location in locations:
            #logging.info(location)
            self.sites[location] = 0

        count = 0
        for sub in subscriptions:
            subscription = Subscription(id = sub)
            subscription.loadData()
            #logging.info("I am processing subscription %i" %(subscription))
            for location in locations:
                count += 1
                #logging.info("I have done this %s times" %(count))
                for state in jobStates:
                    #logging.info('I should be doing something for state %s' %(state))
                    value = subscription.getNumberOfJobsPerSite(location = location, state = state)
                    #value = jobLocate.execute(location = location, subscription = subscription, state = state).values()[0]
                    self.sites[location] = self.sites[location] + value
                logging.info("There are now %i jobs for site %s with value %i" %(self.sites[location], location, value))

        #logging.info(myThread.dbi.processData("SELECT id FROM wmbs_jobgroup", {})[0].fetchall())

            
        #You should now have a count of all jobs in self.sites

        return
                
            


    def pollSites(self):
        """
        Look for the total number of active sites.
        
        This must be run AFTER pollJobs!

        """


        #I think this is supposed to be done using siteDB
        #We're getting this from WMCore.Services

        myThread = threading.currentThread()

        logging.info('Started pollSites')

        siteDBconfig = {}

        #Don't uncomment this unless you want to overwrite the siteDB service logger
        #siteDBconfig['logger'] =  logging

        siteDB = JCSDBInterface(siteDBconfig)


        for location in self.sites.keys():
            slots = siteDB.getPledgedSlots(location)
            self.slots[location] = slots

        return


    def askWorkQueue(self):
        """
        This probably does more then it should: it checks every site, and then decides whether they're full or not

        """

        logging.info('Off to see the work queue')

        workQueueDict = {}

        for location in self.sites.keys():
            #This is the number of free slots - the number of Created but not Exectuing jobs
            freeSlots = self.slots[location] - self.sites[location]
            freeSlots *= self.topOffFactor

            #I need freeSlots jobs on site location
            logging.info('I need %s jobs on site%s' %(freeSlots, location))

            workQueueDict[location] = freeSlots


        #workQueue.askWork(workQueueDict)


        return

        



    #Assistance functions



    def retrieveJobSplitParams(self, subscription):
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


        workflow = subscription['workflow']
        workflow.load()
        wmWorkloadURL = workflow.spec

        if not os.path.isfile(wmWorkloadURL):
            return {"files_per_job" : 5}

        wmWorkload = WMWorkloadHelper(WMWorkload("workload"))
        wmWorkload.load(wmWorkloadURL)

        splitDict = wmWorkload.data.split.splitParams

        jobSplitParams = {}

        if splitDict.has_key("files_per_job"):
            jobSplitParams["files_per_job"] = splitDict["files_per_job"]
        if splitDict.has_key("min_merge_size"):
            jobSplitParams["min_merge_size"] = splitDict["min_merge_size"]
        if splitDict.has_key("max_merge_size"):
            jobSplitParams["max_merge_size"] = splitDict["max_merge_size"]
        if splitDict.has_key("max_merge_events"):
            jobSplitParams["max_merge_events"] = splitDict["max_merge_events"]            

        return jobSplitParams




    def createJobGroup(self, wmbsJobGroup, jobGroupConfig, wmbsSubscription):
        """
        Pass this on to the jobCreator, which actually does the work
        
        """

        myThread = threading.currentThread()



        myThread.transaction.begin()

        logging.info('I am in createJobGroup for jobGroup %i' %(wmbsJobGroup.id))

        #Here things get interesting.
        #We assume that this follows the basic scheme for the jobGroups, that each jobGroup contains
        #files with only one set of sites.
        #Using this we can determine the number of free slots for each job.


        for job in wmbsJobGroup.jobs:
            logging.info('Saving jobs for %i in jobGroup %i' %(wmbsSubscription['id'], wmbsJobGroup.id))
            job["location"] = self.findSiteForJob(job)
            self.sites[job["location"]] += 1
            job.save()

        print self.sites
        print self.slots

        myThread.transaction.commit()

        cfg_params = {}

        #This does nothing
        logging.info("Submitting jobs")
        jobSubmitter = JobSubmitter(self.config)
        jobSubmitter.configure(cfg_params)
        jobSubmitter.submitJobs(jobGroup = wmbsJobGroup, jobGroupConfig = jobGroupConfig)

        logging.info("JobCreator has submitted jobs and is ending")


        return

    def findSiteForJob(self, job):
        """
        _findSiteForJob_

        This searches all known sites and finds the best match for this job
        """

        myThread = threading.currentThread()

        sites = list(job.getFiles()[0]['locations'])

        tmpSite  = ''
        tmpSlots = 0
        for loc in sites:
            if not loc in self.slots.keys() and not loc in self.sites.keys():
                logging.error('Found job for unknown site %s' %(loc))
                logging.error('ABORT: Am not processing jobGroup %i' %(wmbsJobGroup.id))
                return
            if self.slots[loc] - self.sites[loc] > tmpSlots:
                tmpSlots = self.slots[loc] - self.sites[loc]
                tmpSite  = loc

        return tmpSite


            

