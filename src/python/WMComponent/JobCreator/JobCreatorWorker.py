#!/usr/bin/env python
#pylint: disable-msg=E1101
#E1101 doesn't allow you to define config sections using .section_()
"""
The JobCreator Poller for the JSM
"""
__all__ = []
__revision__ = "$Id: JobCreatorWorker.py,v 1.8 2010/04/26 20:37:44 mnorman Exp $"
__version__ = "$Revision: 1.8 $"

import threading
import logging
import os
import os.path
import cProfile, pstats

import cPickle


from WMCore.WorkerThreads.BaseWorkerThread  import BaseWorkerThread
from WMCore.DAOFactory                      import DAOFactory
from WMCore.JobSplitting.SplitterFactory    import SplitterFactory
from WMCore.WMBS.Subscription               import Subscription
from WMCore.WMBS.Workflow                   import Workflow
from WMCore.WMSpec.WMWorkload               import WMWorkload, WMWorkloadHelper
from WMCore.ThreadPool                      import WorkQueue
from WMCore.Database.Transaction            import Transaction
                                            
                                            
from WMCore.WMSpec.Seeders.SeederManager                import SeederManager
from WMCore.JobStateMachine.ChangeState                 import ChangeState
from WMCore.WMSpec.Makers.Interface.CreateWorkArea      import CreateWorkArea

from WMCore.Agent.Configuration import Configuration

class JobCreatorWorker:

    def __init__(self, **configDict):
        """
        init jobCreator
        """

        myThread = threading.currentThread()

        self.transaction = myThread.transaction

        #DAO factory for WMBS objects
        self.daoFactory = DAOFactory(package = "WMCore.WMBS", logger = logging, dbinterface = myThread.dbi)

        # WMCore splitter factory for splitting up jobs.
        self.splitterFactory = SplitterFactory()

        #Dictionaries to be filled later
        self.sites         = {}
        self.slots         = {}
        self.workflows     = {}
        self.subscriptions = {}


        config = Configuration()
        config.section_("JobStateMachine")
        config.JobStateMachine.couchurl      = configDict["couchURL"]
        config.JobStateMachine.couch_retries = configDict["defaultRetries"]
        config.JobStateMachine.couchDBName   = configDict["couchDBName"]

        self.config = config

        #Variables
        self.jobCacheDir    = configDict['jobCacheDir']
        self.defaultJobType = configDict['defaultJobType']


        
        self.createWorkArea  = CreateWorkArea()

        return


    def __call__(self, parameters):
        """
        Poller for looking in all active subscriptions for jobs that need to be made.

        """

        logging.info("In JobCreatorWorker.__call__")

        myThread = threading.currentThread()

        subscriptionID = parameters.get('subscription')

        if subscriptionID == -1:
            return subscriptionID

        myThread.transaction.commit()

        myThread.transaction.begin()

        logging.info("About to call subscription %i" %subscriptionID)

        wmbsSubscription = Subscription(id = subscriptionID)
        wmbsSubscription.load()
        wmbsSubscription["workflow"].load()
        workflow         = wmbsSubscription["workflow"]
        wmWorkload       = self.retrieveWMSpec(wmbsSubscription)

        logging.info("Retrieved WMBS info")

        if not workflow.task or not wmWorkload:
            wmTask = None
            seederList = []
        else:
            wmTask = wmWorkload.getTaskByPath(workflow.task)
            if hasattr(wmTask.data, 'seeders'):
                manager    = SeederManager(wmTask)
                seederList = manager.getSeederList()
            else:
                seederList = []

        logging.info("About to enter JobFactory")

        #My hope is that the job factory is smart enough only to split un-split jobs
        wmbsJobFactory = self.splitterFactory(package = "WMCore.WMBS", \
                                              subscription = wmbsSubscription, generators=seederList)
        splitParams = self.retrieveJobSplitParams(wmWorkload, workflow.task)
        logging.debug("Split Params: %s" % splitParams)
        wmbsJobGroups = wmbsJobFactory(**splitParams)
        logging.debug("Job Groups %s" % wmbsJobGroups)
        logging.info("Have jobGroups")

        # Now we get to find out what job they are.
        countJobs = self.daoFactory(classname = "Jobs.GetNumberOfJobsPerWorkflow")
        jobNumber = countJobs.execute(workflow = workflow.id)

        myThread.transaction.commit()

        for wmbsJobGroup in wmbsJobGroups:
            self.createJobGroup(wmbsJobGroup)
            #Create a directory
            self.createWorkArea.processJobs(jobGroupID = wmbsJobGroup.exists(), \
                                            startDir = self.jobCacheDir)

            for job in wmbsJobGroup.jobs:
                jobNumber += 1
                #We better save the whole job
                #First, add the necessary components
                if wmTask:
                    #If we managed to load the task, so the url should be valid
                    job['spec']    = workflow.spec
                    job['sandbox'] = wmTask.data.input.sandbox
                    job['task']    = wmTask.getPathName()
                job['number']  = jobNumber
                cacheDir = job.getCache()
                job['cache_dir'] = cacheDir
                output = open(os.path.join(cacheDir, 'job.pkl'),'w')
                cPickle.dump(job, output)
                output.close()
                

            logging.info("Finished call for jobGroup %i" %(wmbsJobGroup.exists()))

        #print "Finished JobCreatorWorker.__call__"

        return subscriptionID




    def retrieveJobSplitParams(self, wmWorkload, task):
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
            return {"files_per_job": 5}
        task = wmWorkload.getTaskByPath(task)
        if not task:
            return {"files_per_job": 5}
        else:
            return task.jobSplittingParameters()


    def createJobGroup(self, wmbsJobGroup):
        """
        Pass this on to the jobCreator, which actually does the work
        
        """

        myThread = threading.currentThread()

        myThread.transaction.begin()

        changeState = ChangeState(self.config)

        #Create the job
        changeState.propagate(wmbsJobGroup.jobs, 'created', 'new')
        myThread.transaction.commit()

        logging.info("JobCreator has changed jobs to Created for jobGroup %i and is ending" %(wmbsJobGroup.id))


        return



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
