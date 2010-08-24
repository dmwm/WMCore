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



import threading
import logging
import os
import os.path


import gc

import cPickle


from WMCore.DAOFactory                      import DAOFactory
from WMCore.JobSplitting.SplitterFactory    import SplitterFactory
from WMCore.WMBS.Subscription               import Subscription
from WMCore.WMSpec.WMWorkload               import WMWorkload, WMWorkloadHelper
                                            
                                            
from WMCore.WMSpec.Seeders.SeederManager                import SeederManager
from WMCore.JobStateMachine.ChangeState                 import ChangeState
from WMComponent.JobCreator.CreateWorkArea              import CreateWorkArea

from WMCore.Agent.Configuration import Configuration


#pylint: disable-msg=C0103
# I borrowed this code from elsewhere
# So I'm not changing it
# See: http://code.activestate.com/recipes/286222-memory-usage/

def _VmB(VmKey):
    '''Private.
    Code from http://code.activestate.com/recipes/286222-memory-usage/
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


#pylint: enable-msg=C0103

def retrieveWMSpec(subscription):
    """
    _retrieveWMSpec_
    
    Given a subscription, this function loads the WMSpec associated with that workload
    """
    workflow = subscription['workflow']
    wmWorkloadURL = workflow.spec
    
    if not os.path.isfile(wmWorkloadURL):
        logging.error("WMWorkloadURL %s is empty" % (wmWorkloadURL))
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
        logging.error("Could not find wmWorkload for splitting")
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


def doMemoryCheck(msgString):
    """
    _doMemoryCheck_
    
    Check the memory usage
    Print to log debug
    """

    logging.debug(msgString)
    logging.debug(_VmB('VmSize:'))
    logging.debug(_VmB('VmRSS:'))
    logging.debug(_VmB('VmStk:'))
    logging.debug(gc.get_count())

    return




    

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

        self.changeState = ChangeState(self.config)

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

            if subscriptionID < 0:
                logging.error("Got non-existant subscription")
                logging.error("Assuming parameters in error: returning")
                return subscriptionID
            
            myThread.transaction.begin()
            
            logging.info("About to call subscription %i" %subscriptionID)
            
            wmbsSubscription = Subscription(id = subscriptionID)
            wmbsSubscription.load()
            wmbsSubscription["workflow"].load()
            workflow         = wmbsSubscription["workflow"]

            wmWorkload       = retrieveWMSpec(wmbsSubscription)



            

            if not workflow.task or not wmWorkload:
                # Then we have a problem
                # We have no sandbox
                # We NEED a sandbox
                # Abort this subscription!
                # But do NOT fail
                # We have no way of marking a subscription as bad per se
                # We'll have to just keep skipping it
                wmTask = None
                seederList = []
                logging.error("Have no task for workflow %i" % (workflow.id))
                logging.error("Aborting Subscription %i" % (subscriptionID))
                continue
                
            else:
                wmTask = wmWorkload.getTaskByPath(workflow.task)
                if hasattr(wmTask.data, 'seeders'):
                    manager    = SeederManager(wmTask)
                    seederList = manager.getSeederList()
                else:
                    seederList = []

            logging.info("About to enter JobFactory")
            logging.debug("Going to call wmbsJobFactory with limit %i" % (self.limit))
            
            # My hope is that the job factory is smart enough only to split un-split jobs
            wmbsJobFactory = self.splitterFactory(package = "WMCore.WMBS",
                                                  subscription = wmbsSubscription,
                                                  generators=seederList,
                                                  limit = self.limit)
            splitParams = retrieveJobSplitParams(wmWorkload, workflow.task)
            logging.debug("Split Params: %s" % splitParams)

            continueSubscription = True
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
                    logging.info("Retrieved %i jobGroups from jobSplitter" % (len(wmbsJobGroups)))
                except StopIteration:
                    # If you receive a stopIteration, we're done
                    logging.info("Completed iteration over subscription %i" % (subscriptionID))
                    continueSubscription = False
                    continue

                # Now we get to find out what job they are.
                countJobs = self.daoFactory(classname = "Jobs.GetNumberOfJobsPerWorkflow")
                jobNumber = countJobs.execute(workflow = workflow.id)
                logging.debug("Have %i jobs for this workflow already" % (jobNumber))
                
                
            
                for wmbsJobGroup in wmbsJobGroups:

                    logging.debug("Processing jobGroup %i" % (wmbsJobGroup.exists()))
                    logging.debug("Processing %i jobs" % (len(wmbsJobGroup.jobs)) )
                
                    # Create a directory
                    self.createWorkArea.processJobs(jobGroup = wmbsJobGroup,
                                                    startDir = self.jobCacheDir,
                                                    workflow = workflow,
                                                    wmWorkload = wmWorkload)

                    
                    for job in wmbsJobGroup.jobs:
                        jobNumber += 1
                        self.saveJob(job = job, workflow = workflow,
                                     wmTask = wmTask, jobNumber = jobNumber)
            

                    self.createJobGroup(wmbsJobGroup)

                    logging.debug("Finished call for jobGroup %i" \
                                 % (wmbsJobGroup.exists()))


                # END: while loop over jobSplitter



            # About to reset everything
            wmbsJobGroups  = None
            wmTask         = None
            wmWorkload     = None
            splitParams    = None
            wmbsJobFactory = None
            gc.collect()



            # About to check memory
            doMemoryCheck("About to get memory references: End of subscription loop")


        # Final memory check
        doMemoryCheck("About to get memory references: End of __call__()")


        logging.debug("About to return from JobCreatorWorker.__call__()")

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
            job['task']    = wmTask.getPathName()
            if job.get('sandbox', None) == None:
                job['sandbox'] = wmTask.data.input.sandbox

        job['counter']  = jobNumber
        cacheDir = job.getCache()
        job['cache_dir'] = cacheDir
        output = open(os.path.join(cacheDir, 'job.pkl'), 'w')
        cPickle.dump(job, output, cPickle.HIGHEST_PROTOCOL)
        output.close()


        return

    


    def createJobGroup(self, wmbsJobGroup):
        """
        Pass this on to the jobCreator, which actually does the work
        
        """

        myThread = threading.currentThread()

        myThread.transaction.begin()



        #Create the job
        self.changeState.propagate(wmbsJobGroup.jobs, 'created', 'new')
        myThread.transaction.commit()

        logging.info("JobCreator has created jobGroup %i and is ending" \
                     % (wmbsJobGroup.id))


        return


    



    
