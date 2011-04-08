#!/usr/bin/env python
#pylint: disable-msg=W0613, W6501
"""
The JobCreator Poller for the JSM
"""
__all__ = []


import os
import copy
import time
import Queue
import os.path
import cPickle
import logging
import traceback
import threading
import multiprocessing
#import time
#import cProfile, pstats


from WMCore.WorkerThreads.BaseWorkerThread  import BaseWorkerThread
from WMCore.DAOFactory                      import DAOFactory
from WMCore.WMException                     import WMException


from WMCore.ProcessPool.ProcessPool                     import ProcessPool

from WMCore.WMSpec.Seeders.SeederManager                import SeederManager
from WMCore.JobStateMachine.ChangeState                 import ChangeState
from WMComponent.JobCreator.CreateWorkArea              import CreateWorkArea

from WMCore.JobSplitting.SplitterFactory    import SplitterFactory
from WMCore.WMBS.Subscription               import Subscription
from WMCore.WMBS.Workflow                   import Workflow
from WMCore.WMSpec.WMWorkload               import WMWorkload, WMWorkloadHelper


def retrieveWMSpec(workflow):
    """
    _retrieveWMSpec_
    
    Given a subscription, this function loads the WMSpec associated with that workload
    """
    #workflow = subscription['workflow']
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
        # Dump it after one go if we're not grabbing by proxy
        if jobFactory.grabByProxy == False:
            break



def saveJob(job, workflow, sandbox, wmTask = None, jobNumber = 0,
            wmTaskPrio = None, owner = None, ownerDN = None ):
        """
        _saveJob_

        Actually do the mechanics of saving the job to a pickle file
        """
        if wmTask:
            # If we managed to load the task,
            # so the url should be valid
            job['spec']     = workflow.spec
            job['task']     = wmTask
            if job.get('sandbox', None) == None:
                job['sandbox'] = sandbox

        job['counter']   = jobNumber
        cacheDir         = job.getCache()
        job['cache_dir'] = cacheDir
        job['priority']  = wmTaskPrio
        job['owner']     = owner
        job['ownerDN']     = ownerDN
        output = open(os.path.join(cacheDir, 'job.pkl'), 'w')
        cPickle.dump(job, output, cPickle.HIGHEST_PROTOCOL)
        output.close()


        return


def creatorProcess(work, jobCacheDir):
    """
    _creatorProcess_

    Creator work areas and pickle job objects
    """
    createWorkArea  = CreateWorkArea()
    
    try:
        wmbsJobGroup = work.get('jobGroup')
        workflow     = work.get('workflow')
        wmWorkload   = work.get('wmWorkload')
        wmTaskName   = work.get('wmTaskName')
        sandbox      = work.get('sandbox')
        owner        = work.get('owner')
        ownerDN      = work.get('ownerDN',None)

        if ownerDN == None:
            ownerDN = owner
            
        jobNumber    = work.get('jobNumber', 0)
        wmTaskPrio   = work.get('wmTaskPrio', None)
    except KeyError, ex:
        msg =  "Could not find critical key-value in work input.\n"
        msg += str(ex)
        logging.error(msg)
        raise JobCreatorException(msg)
    except Exception, ex:
        msg =  "Exception in opening work package.\n"
        msg += str(ex)
        msg += str(traceback.format_exc())
        logging.error(msg)
        raise JobCreatorException(msg)


    try:
        createWorkArea.processJobs(jobGroup = wmbsJobGroup,
                                   startDir = jobCacheDir,
                                   workflow = workflow,
                                   wmWorkload = wmWorkload,
                                   cache = False)

        for job in wmbsJobGroup.jobs:
            jobNumber += 1
            saveJob(job = job, workflow = workflow,
                    wmTask = wmTaskName,
                    jobNumber = jobNumber,
                    wmTaskPrio = wmTaskPrio,
                    sandbox = sandbox,
                    owner = owner,
                    ownerDN = ownerDN)

    except Exception, ex:
        # Register as failure; move on
        msg =  "Exception in processing wmbsJobGroup %i\n" % wmbsJobGroup.id
        msg += str(ex)
        msg += str(traceback.format_exc())
        logging.error(msg)
        raise JobCreatorException(msg)

    return wmbsJobGroup
        
        

# This is the code for the multiprocessing based creator
# It's kept around so I can remember how I arranged the exception tree
# Keep this until we make a decision about large-scale transactions
# in the JobCreator.

#def creatorProcess(input, result, jobCacheDir):
#    """
#    _creatorProcess_
#    
#    Run the CreateWorkArea code
#    """
#
#
#    createWorkArea  = CreateWorkArea()
#
#
#    while True:
#
#        try:
#            work = input.get()
#        except (EOFError, IOError):
#            crashMessage =  "Hit EOF/IO in getting new work\n"
#            crashMessage += "Assuming this is a graceful break attempt.\n"
#            logging.error(crashMessage)
#            break
#
#        if work == 'STOP':
#            # Put the brakes on
#            break
#
#        try:
#            wmbsJobGroup = work.get('jobGroup')
#            workflow     = work.get('workflow')
#            wmWorkload   = work.get('wmWorkload')
#            wmTaskName   = work.get('wmTaskName')
#            sandbox      = work.get('sandbox')
#            jobNumber    = work.get('jobNumber', 0)
#            wmTaskPrio   = work.get('wmTaskPrio', None)
#        except KeyError, ex:
#            msg =  "Could not find critical key-value in work input.\n"
#            msg += str(ex)
#            logging.error(msg)
#            result.put({status: 'Error', 'msg': msg})
#            continue
#        except Exception, ex:
#            msg =  "Exception in opening work package.\n"
#            msg += str(ex)
#            msg += str(traceback.format_exc())
#            logging.error(msg)
#            result.put({'success': False, 'msg': msg})
#            continue
#
#
#        try:
#            #print "Retrieved work in creatorProcess %i: %s" % (os.getpid(), [x['id'] for x in wmbsJobGroup.jobs])
#            createWorkArea.processJobs(jobGroup = wmbsJobGroup,
#                                       startDir = jobCacheDir,
#                                       workflow = workflow,
#                                       wmWorkload = wmWorkload,
#                                       cache = False)
#
#            for job in wmbsJobGroup.jobs:
#                jobNumber += 1
#                saveJob(job = job, workflow = workflow,
#                        wmTask = wmTaskName,
#                        jobNumber = jobNumber,
#                        wmTaskPrio = wmTaskPrio,
#                        sandbox = sandbox)
#
#            # If we got this far, it should be a success
#            result.put({'success': True, 'jobGroup': wmbsJobGroup})
#
#        except Exception, ex:
#            # Register as failure; move on
#            msg =  "Exception in processing wmbsJobGroup %i\n" % wmbsJobGroup.id
#            msg += str(ex)
#            msg += str(traceback.format_exc())
#            logging.error(msg)
#            result.put({'success': False, 'msg': msg})
#
#        #print "Finished work in creatorProcess %i" % os.getpid()
#        #print [x['id'] for x in wmbsJobGroup.jobs]
#
#        # END while loop that does the work
#
#    return 0

    



    
        


class JobCreatorException(WMException):
    """
    _JobCreatorException_

    Specific JobCreatorPoller exception handling.
    If we ever need it.
    """
    


class JobCreatorPoller(BaseWorkerThread):

    """
    Poller that does the work of job creation.
    Polls active subscriptions, asks for more work, and checks with local sites.

    """


    def __init__(self, config):
        """
        init jobCreator
        """

        BaseWorkerThread.__init__(self)

        myThread = threading.currentThread()

        #DAO factory for WMBS objects
        self.daoFactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = logging,
                                     dbinterface = myThread.dbi)

        self.splitterFactory  = SplitterFactory()
        self.setBulkCache     = self.daoFactory(classname = "Jobs.SetCache")
        self.countJobs        = self.daoFactory(classname = "Jobs.GetNumberOfJobsPerWorkflow")
        self.subscriptionList = self.daoFactory(classname = "Subscriptions.ListIncomplete")
        
        #information
        self.config = config

        #Variables
        self.defaultJobType     = config.JobCreator.defaultJobType
        self.limit              = getattr(config.JobCreator, 'fileLoadLimit', 500)

        try:
            self.jobCacheDir        = getattr(config.JobCreator, 'jobCacheDir',
                                              os.path.join(config.JobCreator.componentDir, 'jobCacheDir'))
            self.check()
        except WMException:
            raise
        except Exception, ex:
            msg =  "Unhandled exception while setting up jobCacheDir!\n"
            msg += str(ex)
            logging.error(msg)
            raise JobCreatorException(msg)

        
        self.changeState = ChangeState(self.config)

        return

    def check(self):
        """
        Initial sanity checks on necessary environment factors

        """

        if not os.path.isdir(self.jobCacheDir):
            if not os.path.exists(self.jobCacheDir):
                os.makedirs(self.jobCacheDir)
            else:
                msg = "Assigned a pre-existant cache object %s.  Failing!" \
                      % (self.jobCacheDir)
                raise JobCreatorException (msg)


    def algorithm(self, parameters = None):
        """
        Actually runs the code
        """
        logging.debug("Running JSM.JobCreator")
        try:
            self.pollSubscriptions()
        except WMException:
            #self.close()
            myThread = threading.currentThread()
            if getattr(myThread, 'transaction', False) \
                   and getattr(myThread.transaction, 'transaction', False):
                myThread.transaction.rollback()
            raise
        except Exception, ex:
            #self.close()
            myThread = threading.currentThread()
            if getattr(myThread, 'transaction', False) \
                   and getattr(myThread.transaction, 'transaction', False):
                myThread.transaction.rollback()
            msg = "Failed to execute JobCreator \n%s\n" % (ex)
            raise JobCreatorException(msg)

    def terminate(self, params):
        """
        _terminate_
        
        Kill the code after one final pass when called by the master thread.
        """
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)


    def pollSubscriptions(self):
        """
        Poller for looking in all active subscriptions for jobs that need to be made.

        """
        logging.info("Beginning JobCreator.pollSubscriptions() cycle.")
        myThread = threading.currentThread()

        #First, get list of Subscriptions
        subscriptions    = self.subscriptionList.execute()

        # Okay, now we have a list of subscriptions
        for subscriptionID in subscriptions:
            wmbsSubscription = Subscription(id = subscriptionID)
            try:
                wmbsSubscription.load()
            except IndexError:
                # This happens when the subscription no longer exists
                # i.e., someone executed a kill() function on the database
                # while the JobCreator was in cycle
                # Ignore this subscription
                logging.error("JobCreator cannot load subscription %i" % subscriptionID)
                continue

            workflow         = Workflow(id = wmbsSubscription["workflow"].id)
            workflow.load()
            wmbsSubscription['workflow'].name = workflow.name
            wmWorkload       = retrieveWMSpec(workflow = workflow)

            if not workflow.task or not wmWorkload:
                # Then we have a problem
                # We NEED a sandbox
                # Abort this subscription!
                # But do NOT fail
                # We have no way of marking a subscription as bad per se
                # We'll have to just keep skipping it
                logging.error("Have no task for workflow %i" % (workflow.id))
                logging.error("Aborting Subscription %i" % (subscriptionID))
                continue

            logging.debug("Have loaded subscription %i with workflow %i\n" % (subscriptionID, workflow.id))

            # Set task object
            wmTask = wmWorkload.getTaskByPath(workflow.task)
            if hasattr(wmTask.data, 'seeders'):
                manager    = SeederManager(wmTask)
                seederList = manager.getSeederList()
            else:
                seederList = []

            logging.debug("Going to call wmbsJobFactory for sub %i with limit %i" % (subscriptionID, self.limit))
            
            # My hope is that the job factory is smart enough only to split un-split jobs
            wmbsJobFactory = self.splitterFactory(package = "WMCore.WMBS",
                                                  subscription = wmbsSubscription,
                                                  generators=seederList,
                                                  limit = self.limit)
            splitParams = retrieveJobSplitParams(wmWorkload, workflow.task)
            logging.debug("Split Params: %s" % splitParams)

            # Turn on the jobFactory
            wmbsJobFactory.open()

            # Create a function to hold it
            jobSplittingFunction = runSplitter(jobFactory = wmbsJobFactory,
                                               splitParams = splitParams)

            # Now we get to find out how many jobs there are.
            jobNumber = self.countJobs.execute(workflow = workflow.id,
                                               conn = myThread.transaction.conn, 
                                               transaction = True)
            jobNumber += splitParams.get('initial_lfn_counter', 0)
            logging.debug("Have %i jobs for this workflow already" % (jobNumber))

            continueSubscription = True
            while continueSubscription:
                # This loop runs over the jobFactory,
                # using yield statements and a pre-existing proxy to
                # generate and process new jobs

                # First we need the jobs.
                myThread.transaction.begin()
                try:
                    wmbsJobGroups = jobSplittingFunction.next()
                    logging.info("Retrieved %i jobGroups from jobSplitter" % (len(wmbsJobGroups)))
                except StopIteration:
                    # If you receive a stopIteration, we're done
                    logging.info("Completed iteration over subscription %i" % (subscriptionID))
                    continueSubscription = False
                    myThread.transaction.commit()
                    break

                # If we have no jobGroups, we're done
                if len(wmbsJobGroups) == 0:
                    logging.info("Found end in iteration over subscription %i" % (subscriptionID))
                    continueSubscription = False
                    myThread.transaction.commit()
                    break

                            
                # Assemble a dict of all the info
                processDict = {'workflow': workflow,
                               'wmWorkload': wmWorkload, 'wmTaskName': wmTask.getPathName(),
                               'jobNumber': jobNumber, 'sandbox': wmTask.data.input.sandbox,
                               'wmTaskPrio': wmTask.getTaskPriority(),
                               'owner': wmWorkload.getOwner().get('name', None),
                               'ownerDN': wmWorkload.getOwner().get('dn', None)}
                tempSubscription = Subscription(id = wmbsSubscription['id'])

                nameDictList = []
                for wmbsJobGroup in wmbsJobGroups:
                    # For each jobGroup, put a dictionary
                    # together and run it with creatorProcess
                    jobsInGroup               = len(wmbsJobGroup.jobs)
                    wmbsJobGroup.subscription = tempSubscription
                    tempDict = {}
                    tempDict.update(processDict)
                    tempDict['jobGroup'] = wmbsJobGroup
                    jobGroup = creatorProcess(work = tempDict,
                                              jobCacheDir = self.jobCacheDir)
                    jobNumber += jobsInGroup

                    # Set jobCache for group
                    for job in jobGroup.jobs:
                        nameDictList.append({'jobid':job['id'],
                                             'cacheDir':job['cache_dir']})
                        job["user"] = wmWorkload.getOwner()["name"]
                        job["group"] = wmWorkload.getOwner()["group"]
                        job["taskType"] = wmTask.taskType()
                # Set the caches in the database
                try:
                    if len(nameDictList) > 0:
                        self.setBulkCache.execute(jobDictList = nameDictList,
                                                  conn = myThread.transaction.conn, 
                                                  transaction = True)
                except WMException:
                    raise
                except Exception, ex:
                    msg =  "Unknown exception while setting the bulk cache:\n"
                    msg += str(ex)
                    logging.error(msg)
                    logging.debug("Error while setting bulkCache with following values: %s\n" % nameDictList)
                    raise JobCreatorException(msg)

                # Advance the jobGroup in changeState
                for wmbsJobGroup in wmbsJobGroups:
                    self.advanceJobGroup(wmbsJobGroup = wmbsJobGroup)

                # Now end the transaction so that everything is wrapped
                # in a single rollback
                myThread.transaction.commit()


            # END: While loop over jobFactory

            # Close the jobFactory
            wmbsJobFactory.close()
            
        return
    

# This is the code for the multiprocessing based queue retrieval system
# I'm keeping this here because I hope to go back and re-instate this once
# I figure out how to deal with the transaction problems.
# Keep until we can assure ourselves that what we're doing now presents no problems.

#        while True:
#            try:
#                # Get stuff out of the queue with a ridiculously
#                # short wait time
#                startTime = time.time()
#                entry = self.result.get(timeout = self.wait)
#
#                if not entry.get('success', False):
#                    msg =  "Encountered exception processing jobGroup\n"
#                    msg += entry.get('msg', '')
#                    logging.error(msg)
#                    continue
#                else:
#                    wmbsJobGroup = entry.get('jobGroup')
#            except Queue.Empty:
#                # This means the queue has no current results
#                stopTime  = time.time()
#                logging.error("Found empty queue after %f seconds" % (stopTime - startTime))
#                break
#
#            # Set the cache dir
#            nameDictList = []
#            for job in wmbsJobGroup.jobs:
#                nameDictList.append({'jobid':job['id'], 'cacheDir':job['cache_dir']})
#
#            
#            try:
#                myThread.transaction.begin()
#                self.setBulkCache.execute(jobDictList = nameDictList,
#                                          conn = myThread.transaction.conn, 
#                                          transaction = True)
#                myThread.transaction.commit()
#            except Exception, ex:
#                msg =  "Unhandled exception while setting jobCache for jobGroup %i\n" % wmbsJobGroup.id
#                msg += str(ex)
#                logging.error(msg)
#                raise JobCreatorException(msg)
#                
#            self.advanceJobGroup(wmbsJobGroup = wmbsJobGroup)
#
#            logging.debug("Finished call for jobGroup %i" \
#                          % (wmbsJobGroup.id))
#
#        #END: While loop over wmbsJob
#            
#        return


    def advanceJobGroup(self, wmbsJobGroup):
        """
        _advanceJobGroup_
        
        Mark jobGroup as ready in changeState        
        """
        try:
            self.changeState.propagate(wmbsJobGroup.jobs, 'created', 'new')
        except WMException:
            raise
        except Exception, ex:
            msg =  "Unhandled exception while calling changeState.\n"
            msg += str(ex)
            logging.error(msg)
            logging.debug("Error while using changeState on the following jobs: %s\n" % wmbsJobGroup.jobs)

        logging.info("JobCreator has finished creating jobGroup %i.\n" \
                     % (wmbsJobGroup.id))
        return






















