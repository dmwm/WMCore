#!/usr/bin/env python
"""
The JobCreator Poller for the JSM
"""

from builtins import next

__all__ = []

import logging
import os
import os.path
import threading
import pickle

from Utils.Timers import timeFunction
from Utils.PythonVersion import HIGHEST_PICKLE_PROTOCOL
from Utils.MathUtils import quantize
from WMComponent.JobCreator.CreateWorkArea import CreateWorkArea
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.DAOFactory import DAOFactory
from WMCore.WMException import WMException
from WMCore.JobSplitting.Generators.GeneratorManager import GeneratorManager
from WMCore.JobStateMachine.ChangeState import ChangeState
from WMCore.JobSplitting.SplitterFactory import SplitterFactory
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMSpec.WMWorkload import WMWorkload, WMWorkloadHelper
from WMCore.FwkJobReport.Report import Report
from WMCore.WMExceptions import WM_JOB_ERROR_CODES


def retrieveWMSpec(workflow=None, wmWorkloadURL=None):
    """
    _retrieveWMSpec_

    Given a subscription, this function loads the WMSpec associated with that workload
    """
    if not wmWorkloadURL and workflow:
        wmWorkloadURL = workflow.spec

    if not wmWorkloadURL or not os.path.isfile(wmWorkloadURL):
        logging.error("WMWorkloadURL %s is empty", wmWorkloadURL)
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
        if jobFactory.grabByProxy is False:
            break


def capResourceEstimates(jobGroups, constraints):
    """
    _capResourceEstimates_

    Checks the current job resource estimates and cap
    them based on the limits defined in the agent
    config file.
    """
    for jobGroup in jobGroups:
        for j in jobGroup.jobs:
            if not j['estimatedJobTime'] or j['estimatedJobTime'] < constraints['MinWallTimeSecs']:
                j['estimatedJobTime'] = constraints['MinWallTimeSecs']
            if not j['estimatedDiskUsage'] or j['estimatedDiskUsage'] < constraints['MinRequestDiskKB']:
                j['estimatedDiskUsage'] = constraints['MinRequestDiskKB']

            j['estimatedJobTime'] = min(j['estimatedJobTime'], constraints['MaxWallTimeSecs'])
            j['estimatedDiskUsage'] = min(j['estimatedDiskUsage'], constraints['MaxRequestDiskKB'])

            # finally, quantize those
            j['estimatedJobTime'] = quantize(j['estimatedJobTime'], constraints['MinWallTimeSecs'])
            j['estimatedDiskUsage'] = quantize(j['estimatedDiskUsage'], constraints['MinRequestDiskKB'])

    return


def saveJob(job, thisJobNumber, **kwargs):
    """
    _saveJob_

    Actually do the mechanics of saving the job to a pickle file
    """
    job['counter'] = thisJobNumber
    job['spec'] = kwargs.get('workflow').spec
    job['task'] = kwargs.get('wmTaskName')
    job['sandbox'] = kwargs.get('sandbox')
    job['agentNumber'] = kwargs['agentNumber']
    job['agentName'] = kwargs['agentName']
    cacheDir = job.getCache()
    job['cache_dir'] = cacheDir
    job['owner'] = kwargs['owner']
    job['ownerDN'] = kwargs['ownerDN']
    job['ownerGroup'] = kwargs['ownerGroup']
    job['ownerRole'] = kwargs['ownerRole']
    job['scramArch'] = kwargs['scramArch']
    job['swVersion'] = kwargs['swVersion']
    job['numberOfCores'] = kwargs['numberOfCores']
    job['inputDataset'] = kwargs['inputDataset']
    job['inputDatasetLocations'] = kwargs['inputDatasetLocations']
    job['inputPileup'] = kwargs['inputPileup']
    job['allowOpportunistic'] = kwargs['allowOpportunistic']
    job['requiresGPU'] = kwargs['requiresGPU']
    job['gpuRequirements'] = kwargs['gpuRequirements']
    job['requestType'] = kwargs['requestType']

    with open(os.path.join(cacheDir, 'job.pkl'), 'wb') as output:
        pickle.dump(job, output, HIGHEST_PICKLE_PROTOCOL)

    return


def creatorProcess(work, jobCacheDir):
    """
    _creatorProcess_

    Creator work areas and pickle job objects
    """
    createWorkArea = CreateWorkArea()

    try:
        wmbsJobGroup = work.get('jobGroup')
        workflow = work.get('workflow')
        wmWorkload = work.get('wmWorkload')
        work['ownerDN'] = work.get('owner') if work.get('ownerDN', None) is None else work.get('ownerDN')
    except KeyError as ex:
        msg = "Could not find critical key-value in work input.\n"
        msg += str(ex)
        logging.error(msg)
        raise JobCreatorException(msg)
    except Exception as ex:
        msg = "Exception in opening work package. Error: %s" % str(ex)
        logging.exception(msg)
        raise JobCreatorException(msg)

    try:
        createWorkArea.processJobs(jobGroup=wmbsJobGroup,
                                   startDir=jobCacheDir,
                                   workflow=workflow,
                                   wmWorkload=wmWorkload,
                                   cache=False)

        thisJobNumber = work.get('jobNumber', 0)
        for job in wmbsJobGroup.jobs:
            thisJobNumber += 1
            saveJob(job, thisJobNumber, **work)
    except Exception as ex:
        msg = "Exception in processing wmbsJobGroup %i\n. Error: %s" % (wmbsJobGroup.id, str(ex))
        logging.exception(msg)
        raise JobCreatorException(msg)

    return wmbsJobGroup


# This is the code for the multiprocessing based creator
# It's kept around so I can remember how I arranged the exception tree
# Keep this until we make a decision about large-scale transactions
# in the JobCreator.

# def creatorProcess(input, result, jobCacheDir):
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

        # DAO factory for WMBS objects
        self.daoFactory = DAOFactory(package="WMCore.WMBS",
                                     logger=logging,
                                     dbinterface=myThread.dbi)

        self.setBulkCache = self.daoFactory(classname="Jobs.SetCache")
        self.countJobs = self.daoFactory(classname="Jobs.GetNumberOfJobsPerWorkflow")
        self.subscriptionList = self.daoFactory(classname="Subscriptions.ListIncomplete")
        self.setFWJRPath = self.daoFactory(classname="Jobs.SetFWJRPath")

        # information
        self.config = config

        # Variables
        self.defaultJobType = config.JobCreator.defaultJobType
        self.limit = getattr(config.JobCreator, 'fileLoadLimit', 500)
        self.agentNumber = int(getattr(config.Agent, 'agentNumber', 0))
        self.agentName = getattr(config.Agent, 'hostName', '')
        self.glideinLimits = getattr(config.JobCreator, 'GlideInRestriction', None)

        try:
            self.jobCacheDir = getattr(config.JobCreator, 'jobCacheDir',
                                       os.path.join(config.JobCreator.componentDir, 'jobCacheDir'))
            self.check()
        except WMException:
            raise
        except Exception as ex:
            msg = "Unhandled exception while setting up jobCacheDir!\n"
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
                raise JobCreatorException(msg)

    @timeFunction
    def algorithm(self, parameters=None):
        """
        Actually runs the code
        """
        logging.debug("Running JSM.JobCreator")
        try:
            self.pollSubscriptions()
        except WMException:
            # self.close()
            myThread = threading.currentThread()
            if getattr(myThread, 'transaction', False) \
                    and getattr(myThread.transaction, 'transaction', False):
                myThread.transaction.rollback()
            raise
        except Exception as ex:
            # self.close()
            myThread = threading.currentThread()
            if getattr(myThread, 'transaction', False) \
                    and getattr(myThread.transaction, 'transaction', False):
                myThread.transaction.rollback()
            # Handle temporary connection problems (Temporary)
            if "(InterfaceError) not connected" in str(ex):
                logging.error(
                    'There was a connection problem during the JobCreator algorithm, I will try again next cycle')
            else:
                msg = "Failed to execute JobCreator. Error: %s" % str(ex)
                logging.exception(msg)
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

        # First, get list of Subscriptions
        subscriptions = self.subscriptionList.execute()

        # Okay, now we have a list of subscriptions
        for subscriptionID in subscriptions:
            wmbsSubscription = Subscription(id=subscriptionID)
            try:
                wmbsSubscription.load()
            except IndexError:
                # This happens when the subscription no longer exists
                # i.e., someone executed a kill() function on the database
                # while the JobCreator was in cycle
                # Ignore this subscription
                msg = "JobCreator cannot load subscription %i" % subscriptionID
                logging.error(msg)
                continue

            workflow = Workflow(id=wmbsSubscription["workflow"].id)
            workflow.load()
            wmbsSubscription['workflow'] = workflow
            wmWorkload = retrieveWMSpec(workflow=workflow)

            if not workflow.task or not wmWorkload:
                # Then we have a problem
                # We NEED a sandbox
                # Abort this subscription!
                # But do NOT fail
                # We have no way of marking a subscription as bad per se
                # We'll have to just keep skipping it
                msg = "Have no task for workflow %i\n" % (workflow.id)
                msg += "Aborting Subscription %i" % (subscriptionID)
                logging.error(msg)
                continue

            logging.debug("Have loaded subscription %i with workflow %i\n", subscriptionID, workflow.id)

            # retrieve information from the workload to propagate down to the job configuration
            allowOpport = wmWorkload.getAllowOpportunistic()

            # Set task object
            wmTask = wmWorkload.getTaskByPath(workflow.task)

            # Get generators
            # If you fail to load the generators, pass on the job
            try:
                if hasattr(wmTask.data, 'generators'):
                    manager = GeneratorManager(wmTask)
                    seederList = manager.getGeneratorList()
                else:
                    seederList = []
            except Exception as ex:
                msg = "Had failure loading generators for subscription %i\n" % (subscriptionID)
                msg += "Exception: %s\n" % str(ex)
                msg += "Passing over this error.  It will reoccur next interation!\n"
                msg += "Please check or remove this subscription!\n"
                logging.error(msg)
                continue

            logging.debug("Going to call wmbsJobFactory for sub %i with limit %i", subscriptionID, self.limit)

            splitParams = retrieveJobSplitParams(wmWorkload, workflow.task)
            logging.debug("Split Params: %s", splitParams)

            # Load the proper job splitting module
            splitterFactory = SplitterFactory(splitParams.get('algo_package', "WMCore.JobSplitting"))
            # and return an instance of the splitting algorithm
            wmbsJobFactory = splitterFactory(package="WMCore.WMBS",
                                             subscription=wmbsSubscription,
                                             generators=seederList,
                                             limit=self.limit)

            # Turn on the jobFactory --> get available files for that subscription, keep result proxies
            wmbsJobFactory.open()

            # Create a function to hold it, calling __call__ from the JobFactory
            # which then calls algorithm method of the job splitting algo instance
            jobSplittingFunction = runSplitter(jobFactory=wmbsJobFactory,
                                               splitParams=splitParams)

            # Now we get to find out how many jobs there are.
            jobNumber = self.countJobs.execute(workflow=workflow.id,
                                               conn=myThread.transaction.conn,
                                               transaction=True)
            jobNumber += splitParams.get('initial_lfn_counter', 0)
            logging.debug("Have %i jobs for workflow %s already in database.", jobNumber, workflow.name)

            while True:
                # This loop runs over the jobFactory,
                # using yield statements and a pre-existing proxy to
                # generate and process new jobs

                # First we need the jobs.
                myThread.transaction.begin()
                try:
                    wmbsJobGroups = next(jobSplittingFunction)
                    logging.info("Retrieved %i jobGroups from jobSplitter", len(wmbsJobGroups))
                except StopIteration:
                    # If you receive a stopIteration, we're done
                    logging.info("Completed iteration over subscription %i", subscriptionID)
                    myThread.transaction.commit()
                    break

                # If we have no jobGroups, we're done
                if len(wmbsJobGroups) == 0:
                    logging.info("Found end in iteration over subscription %i", subscriptionID)
                    myThread.transaction.commit()
                    break

                # Assemble a dict of all the info
                processDict = {'workflow': workflow,
                               'wmWorkload': wmWorkload,
                               'wmTaskName': wmTask.getPathName(),
                               'requestType': wmWorkload.getRequestType(),
                               'jobNumber': jobNumber,
                               'sandbox': wmTask.data.input.sandbox,
                               'owner': wmWorkload.getOwner().get('name', None),
                               'ownerDN': wmWorkload.getOwner().get('dn', None),
                               'ownerGroup': wmWorkload.getOwner().get('vogroup', ''),
                               'ownerRole': wmWorkload.getOwner().get('vorole', ''),
                               'numberOfCores': wmTask.getNumberOfCores(),
                               'requiresGPU': wmTask.getRequiresGPU(),
                               'gpuRequirements': wmTask.getGPURequirements(),
                               'inputDataset': wmTask.getInputDatasetPath(),
                               'inputPileup': wmTask.getInputPileupDatasets(),
                               'swVersion': wmTask.getSwVersion(allSteps=True),
                               'scramArch': wmTask.getScramArch(),
                               'agentNumber': self.agentNumber,
                               'agentName': self.agentName,
                               'allowOpportunistic': allowOpport}

                tempSubscription = Subscription(id=wmbsSubscription['id'])

                # if we have glideinWMS constraints, then adapt all jobs
                if self.glideinLimits:
                    capResourceEstimates(wmbsJobGroups, self.glideinLimits)

                nameDictList = []
                for wmbsJobGroup in wmbsJobGroups:
                    # For each jobGroup, put a dictionary
                    # together and run it with creatorProcess
                    jobsInGroup = len(wmbsJobGroup.jobs)
                    wmbsJobGroup.subscription = tempSubscription
                    tempDict = {}
                    tempDict.update(processDict)
                    tempDict['jobGroup'] = wmbsJobGroup
                    tempDict['jobNumber'] = jobNumber
                    tempDict['inputDatasetLocations'] = wmbsJobGroup.getLocationsForJobs()

                    jobGroup = creatorProcess(work=tempDict,
                                              jobCacheDir=self.jobCacheDir)
                    jobNumber += jobsInGroup

                    # Set jobCache for group
                    for job in jobGroup.jobs:
                        nameDictList.append({'jobid': job['id'],
                                             'cacheDir': job['cache_dir']})
                        job["user"] = wmWorkload.getOwner()["name"]
                        job["group"] = wmWorkload.getOwner()["group"]
                # Set the caches in the database
                try:
                    if len(nameDictList) > 0:
                        self.setBulkCache.execute(jobDictList=nameDictList,
                                                  conn=myThread.transaction.conn,
                                                  transaction=True)
                except WMException:
                    raise
                except Exception as ex:
                    msg = "Unknown exception while setting the bulk cache:\n"
                    msg += str(ex)
                    logging.error(msg)
                    logging.debug("Error while setting bulkCache with following values: %s\n", nameDictList)
                    raise JobCreatorException(msg)

                # Advance the jobGroup in changeState
                for wmbsJobGroup in wmbsJobGroups:
                    self.advanceJobGroup(wmbsJobGroup=wmbsJobGroup)

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

            createFailedJobs = [x for x in wmbsJobGroup.jobs if x.get('failedOnCreation', False)]
            self.generateCreateFailedReports(createFailedJobs)
            self.changeState.propagate(createFailedJobs, 'createfailed', 'created')
        except WMException:
            raise
        except Exception as ex:
            msg = "Unhandled exception while calling changeState.\n"
            msg += str(ex)
            logging.error(msg)
            logging.debug("Error while using changeState on the following jobs: %s\n", wmbsJobGroup.jobs)

        logging.info("JobCreator has finished creating jobGroup %i.\n", wmbsJobGroup.id)
        return

    def generateCreateFailedReports(self, createFailedJobs):
        """
        _generateCreateFailedReports_

        Create and store FWJR for the  jobs that failed on creation
        leaving meaningful information about what happened with them
        """
        if not createFailedJobs:
            return

        fjrsToSave = []
        for failedJob in createFailedJobs:
            report = Report()
            report.addError("CreationFailure", 99305, "CreationFailure",
                            failedJob.get("failedReason", WM_JOB_ERROR_CODES[99305]))
            jobCache = failedJob.getCache()
            try:
                fjrPath = os.path.join(jobCache, "Report.0.pkl")
                report.save(fjrPath)
                fjrsToSave.append({"jobid": failedJob["id"], "fwjrpath": fjrPath})
                failedJob["fwjr"] = report
            except Exception:
                logging.error("Something went wrong while saving the report for  job %s", failedJob["id"])

        myThread = threading.currentThread()
        self.setFWJRPath.execute(binds=fjrsToSave, conn=myThread.transaction.conn, transaction=True)

        return
