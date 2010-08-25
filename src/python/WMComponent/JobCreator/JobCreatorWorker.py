#!/usr/bin/env python
#pylint: disable-msg=E1101, W6501, W0142, E1103, R0903, R0914
#E1101 doesn't allow you to define config sections using .section_()
#W6501: Allow us to use string formatting for logging messages
#W0142: Use ** magic
#E1103: Transaction attached to myThread
#R0903: You can't win with pylint; it wants more methods, and then
# wants to move them outside the class
#R0914: We just have too many variables to pass normally
"""
The JobCreator Poller for the JSM
"""
__all__ = []
__revision__ = "$Id: JobCreatorWorker.py,v 1.16 2010/08/02 21:14:22 mnorman Exp $"
__version__ = "$Revision: 1.16 $"

import threading
import logging
import os
import os.path


import gc
import sys
import objgraph
import pdb

import cPickle


from WMCore.DAOFactory                      import DAOFactory
from WMCore.JobSplitting.SplitterFactory    import SplitterFactory
from WMCore.WMBS.Subscription               import Subscription
from WMCore.WMSpec.WMWorkload               import WMWorkload, WMWorkloadHelper
                                            
                                            
from WMCore.WMSpec.Seeders.SeederManager                import SeederManager
from WMCore.JobStateMachine.ChangeState                 import ChangeState
from WMCore.WMSpec.Makers.Interface.CreateWorkArea      import CreateWorkArea

from WMCore.Agent.Configuration import Configuration


def _VmB(VmKey):
    '''Private.
    '''
    _proc_status = '/proc/%d/status' % os.getpid()
    _scale = {'kB': 1024.0, 'mB': 1024.0*1024.0,
              'KB': 1024.0, 'MB': 1024.0*1024.0}
    
    # get pseudo file  /proc/<pid>/status
    try:
        t = open(_proc_status)
        v = t.read()
        t.close()
    except:
        return 0.0  # non-Linux?
    # get VmKey line e.g. 'VmRSS:  9999  kB\n ...'
    i = v.index(VmKey)
    v = v[i:].split(None, 3)  # whitespace
    if len(v) < 3:
        return 0.0  # invalid format?
    # convert Vm value to bytes
    return float(v[1]) * _scale[v[2]]

def retrieveWMSpec(subscription):
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


def retrieveJobSplitParams(wmWorkload, task):
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


    # This function has to find the WMSpec, and get the parameters from the spec
    # I don't know where the spec is, but I'll have to find it.
    # I don't want to save it in each workflow area, but I may have to

    if not wmWorkload:
        return {"files_per_job": 5}
    task = wmWorkload.getTaskByPath(task)
    if not task:
        return {"files_per_job": 5}
    else:
        return task.jobSplittingParameters()



def runSplitter(jobFactory, splitParams):
        """
        _runSplitter_

        Run the jobSplitting as a coroutine method, yielding values as required
        """

        groups = ['test']
        while groups != []:
            groups = jobFactory(**splitParams)
            yield groups




    

class JobCreatorWorker:
    """
    This is the ProcessPool worker function that actually
    runs the jobCreator
    """

    def __init__(self, **configDict):
        """
        init jobCreator
        """

        myThread = threading.currentThread()

        self.transaction = myThread.transaction

        #DAO factory for WMBS objects
        self.daoFactory = DAOFactory(package = "WMCore.WMBS", logger = logging,
                                     dbinterface = myThread.dbi)

        # WMCore splitter factory for splitting up jobs.
        self.splitterFactory = SplitterFactory()

        config = Configuration()
        config.section_("JobStateMachine")
        config.JobStateMachine.couchurl      = configDict["couchURL"]
        config.JobStateMachine.couch_retries = configDict["defaultRetries"]
        config.JobStateMachine.couchDBName   = configDict["couchDBName"]

        self.config = config

        #Variables
        self.jobCacheDir    = configDict['jobCacheDir']
        self.defaultJobType = configDict['defaultJobType']
        self.limit          = configDict.get('fileLoadLimit', 500)


        
        self.createWorkArea  = CreateWorkArea()

        return


    def __call__(self, parameters):
        """
        Poller for looking in all active subscriptions for jobs that need to be made.

        """

        logging.info("In JobCreatorWorker.__call__")

        myThread = threading.currentThread()

        for entry in parameters:
            # This retrieves a single subscription
            subscriptionID = entry.get('subscription')

            if subscriptionID == -1:
                return subscriptionID
            
            myThread.transaction.commit()
            
            myThread.transaction.begin()
            
            logging.info("About to call subscription %i" %subscriptionID)
            
            wmbsSubscription = Subscription(id = subscriptionID)
            wmbsSubscription.load()
            wmbsSubscription["workflow"].load()
            workflow         = wmbsSubscription["workflow"]

            #pdb.set_trace()
            
            wmWorkload       = retrieveWMSpec(wmbsSubscription)



            

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

            # My hope is that the job factory is smart enough only to split un-split jobs
            wmbsJobFactory = self.splitterFactory(package = "WMCore.WMBS",
                                                  subscription = wmbsSubscription,
                                                  generators=seederList,
                                                  limit = self.limit)
            splitParams = retrieveJobSplitParams(wmWorkload, workflow.task)
            logging.debug("Split Params: %s" % splitParams)

            continueSubscription = True
            #wmbsJobGroups        = []
            myThread.transaction.commit()

            # Turn on the jobFactory
            wmbsJobFactory.open()

            # Create a function to hold it
            jobSplittingFunction = runSplitter(jobFactory = wmbsJobFactory,
                                               splitParams = splitParams)

            while continueSubscription:
                # This loop runs over the jobFactory,
                # using yield statements and a pre-existing proxy to
                # generate and process new jobs

                # First we need the jobs.
                
                try:
                    wmbsJobGroups = jobSplittingFunction.next()
                    logging.error("Retrieved %i jobGroups from jobSplitter" % (len(wmbsJobGroups)))
                except StopIteration:
                    # If you receive a stopIteration, we're done
                    logging.error("Completed iteration over subscription %i" % (subscriptionID))
                    continueSubscription = False
                    continue

                # wmbsJobGroups = wmbsJobFactory(**splitParams)
                # logging.debug("Job Groups %s" % wmbsJobGroups)
                logging.info("Have jobGroups")
            
                # Now we get to find out what job they are.
                countJobs = self.daoFactory(classname = "Jobs.GetNumberOfJobsPerWorkflow")
                jobNumber = countJobs.execute(workflow = workflow.id)
                
                
            
                for wmbsJobGroup in wmbsJobGroups:
                
                    # Create a directory
                    self.createWorkArea.processJobs(jobGroup = wmbsJobGroup,
                                                    startDir = self.jobCacheDir,
                                                    workflow = workflow,
                                                    wmWorkload = wmWorkload)
                    
                    for job in wmbsJobGroup.jobs:
                        jobNumber += 1
                        self.saveJob(job = job, workflow = workflow,
                                     wmTask = wmTask, jobNumber = jobNumber)
                        ## We better save the whole job
                        ## First, add the necessary components
                        #if wmTask:
                        #    # If we managed to load the task,
                        #    # so the url should be valid
                        #    job['spec']    = workflow.spec
                        #    job['sandbox'] = wmTask.data.input.sandbox
                        #    job['task']    = wmTask.getPathName()
                        #job['counter']  = jobNumber
                        #cacheDir = job.getCache()
                        #job['cache_dir'] = cacheDir
                        #output = open(os.path.join(cacheDir, 'job.pkl'),'w')
                        #cPickle.dump(job, output)
                        #output.close()

                    self.createJobGroup(wmbsJobGroup)

                    logging.info("Finished call for jobGroup %i" \
                                 %(wmbsJobGroup.exists()))


                # END: while loop over jobSplitter



            #wmbsJobFactory.close()
            #logging.error("Ending a loop")
            #logging.error(gc.get_count())
            #logging.error(gc.get_referrers())
            #logging.error(objgraph.show_most_common_types(limit=50))
            #logging.error("About to get memory references")
            #logging.error(_VmB('VmSize:'))
            #logging.error(_VmB('VmRSS:'))
            #logging.error(_VmB('VmStk:'))
            #pdb.set_trace()
            wmbsJobGroups  = None
            wmTask         = None
            wmWorkload     = None
            splitParams    = None
            wmbsJobFactory = None
            gc.collect()
            #logging.error("About to get memory references: Part 2")
            #logging.error(_VmB('VmSize:'))
            #logging.error(_VmB('VmRSS:'))
            #logging.error(_VmB('VmStk:'))
            #logging.error(gc.get_count())


        return parameters


    def saveJob(self, job, workflow, wmTask = None, jobNumber = 0):
        """
        _saveJob_

        Actually do the mechanics of saving the job to a pickle file
        """


        if wmTask:
            # If we managed to load the task,
            # so the url should be valid
            job['spec']    = workflow.spec
            job['sandbox'] = wmTask.data.input.sandbox
            job['task']    = wmTask.getPathName()
        job['counter']  = jobNumber
        cacheDir = job.getCache()
        job['cache_dir'] = cacheDir
        output = open(os.path.join(cacheDir, 'job.pkl'),'w')
        cPickle.dump(job, output)
        output.close()


        return

    


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

        logging.info("JobCreator has changed jobs to Created " \
                     +"for jobGroup %i and is ending" %(wmbsJobGroup.id))


        return


    



    
