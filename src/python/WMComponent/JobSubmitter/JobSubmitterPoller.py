#!/usr/bin/env python
# pylint: disable=C0301
# for the whitelist and the blacklist
# C0301: I'm ignoring this because breaking up error messages is painful
"""
_JobSubmitterPoller_t_

Submit jobs for execution.
"""
from __future__ import print_function, division
from builtins import range
from future.utils import viewitems

import logging
import os.path
import threading
import json
import time
from collections import defaultdict, Counter
import pickle

from Utils.Timers import timeFunction
from WMCore.DAOFactory import DAOFactory
from WMCore.WMExceptions import WM_JOB_ERROR_CODES

from WMCore.JobStateMachine.ChangeState import ChangeState
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.ResourceControl.ResourceControl import ResourceControl
from WMCore.DataStructs.JobPackage import JobPackage
from WMCore.FwkJobReport.Report import Report
from WMCore.WMException import WMException
from WMCore.BossAir.BossAirAPI import BossAirAPI
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr
from WMCore.Services.ReqMgrAux.ReqMgrAux import ReqMgrAux

from WMComponent.JobSubmitter.JobSubmitAPI import availableScheddSlots


def jobSubmitCondition(jobStats):
    for jobInfo in jobStats:
        if jobInfo["Current"] >= jobInfo["Threshold"]:
            return jobInfo["Condition"]

    return "JobSubmitReady"


class JobSubmitterPollerException(WMException):
    """
    _JobSubmitterPollerException_

    This is the exception instance for
    JobSubmitterPoller specific errors.
    """
    pass


class JobSubmitterPoller(BaseWorkerThread):
    """
    _JobSubmitterPoller_

    The jobSubmitterPoller takes the jobs and organizes them into packages
    before sending them to the individual plugin submitters.
    """

    def __init__(self, config):
        BaseWorkerThread.__init__(self)
        myThread = threading.currentThread()
        self.config = config

        # DAO factory for WMBS objects
        self.daoFactory = DAOFactory(package="WMCore.WMBS", logger=logging, dbinterface=myThread.dbi)

        # Libraries
        self.resourceControl = ResourceControl()
        self.changeState = ChangeState(self.config)
        self.bossAir = BossAirAPI(config=self.config, insertStates=True)

        self.hostName = self.config.Agent.hostName
        self.repollCount = getattr(self.config.JobSubmitter, 'repollCount', 10000)
        self.maxJobsPerPoll = int(getattr(self.config.JobSubmitter, 'maxJobsPerPoll', 1000))
        self.maxJobsToCache = int(getattr(self.config.JobSubmitter, 'maxJobsToCache', 50000))
        self.maxJobsThisCycle = self.maxJobsPerPoll  # changes as per schedd limit
        self.cacheRefreshSize = int(getattr(self.config.JobSubmitter, 'cacheRefreshSize', 30000))
        self.skipRefreshCount = int(getattr(self.config.JobSubmitter, 'skipRefreshCount', 20))
        self.packageSize = getattr(self.config.JobSubmitter, 'packageSize', 500)
        self.collSize = getattr(self.config.JobSubmitter, 'collectionSize', self.packageSize * 1000)
        self.maxTaskPriority = getattr(self.config.BossAir, 'maxTaskPriority', 1e7)
        self.condorFraction = 0.75  # update during every algorithm cycle
        self.condorOverflowFraction = 0.2
        self.ioboundTypes = ('LogCollect', 'Merge', 'Cleanup', 'Harvesting')
        self.drainGracePeriod = getattr(self.config.JobSubmitter, 'drainGraceTime', 2 * 24 * 60 * 60)  # 2 days

        # Used for speed draining the agent
        self.enableAllSites = False

        # Additions for caching-based JobSubmitter
        self.jobsByPrio = {}  # key'ed by the final job priority, which contains a set of job ids
        self.jobDataCache = {}  # key'ed by the job id, containing the whole job info dict
        self.jobsToPackage = {}
        self.locationDict = {}
        self.drainSites = dict()
        self.drainSitesSet = set()
        self.abortSites = set()
        self.refreshPollingCount = 0

        try:
            if not getattr(self.config.JobSubmitter, 'submitDir', None):
                self.config.JobSubmitter.submitDir = self.config.JobSubmitter.componentDir
            self.packageDir = os.path.join(self.config.JobSubmitter.submitDir, 'packages')

            if not os.path.exists(self.packageDir):
                os.makedirs(self.packageDir)
        except OSError as ex:
            msg = "Error while trying to create packageDir %s\n!"
            msg += str(ex)
            logging.error(msg)
            logging.debug("PackageDir: %s", self.packageDir)
            logging.debug("Config: %s", config)
            raise JobSubmitterPollerException(msg)

        # Now the DAOs
        self.listJobsAction = self.daoFactory(classname="Jobs.ListForSubmitter")
        self.setLocationAction = self.daoFactory(classname="Jobs.SetLocation")
        self.locationAction = self.daoFactory(classname="Locations.GetSiteInfo")
        self.setFWJRPathAction = self.daoFactory(classname="Jobs.SetFWJRPath")
        self.listWorkflows = self.daoFactory(classname="Workflow.ListForSubmitter")

        # Keep a record of the thresholds in memory
        self.currentRcThresholds = {}

        self.useReqMgrForCompletionCheck = getattr(self.config.TaskArchiver, 'useReqMgrForCompletionCheck', True)

        if self.useReqMgrForCompletionCheck:
            # only set up this when reqmgr is used (not Tier0)
            self.reqmgr2Svc = ReqMgr(self.config.General.ReqMgr2ServiceURL)
            self.abortedAndForceCompleteWorkflowCache = self.reqmgr2Svc.getAbortedAndForceCompleteRequestsFromMemoryCache()
            cacheduration = getattr(self.config.General, "ReqMgrAuxCacheDuration", 5 / 60)  # 5 minutes
            self.reqAuxDB = ReqMgrAux(self.config.General.ReqMgr2ServiceURL, httpDict={'cacheduration': cacheduration})
        else:
            # Tier0 Case - just for the clarity (This private variable shouldn't be used
            self.abortedAndForceCompleteWorkflowCache = None

        return

    def getPackageCollection(self, sandboxDir):
        """
        _getPackageCollection_

        Given a jobID figure out which packageCollection
        it should belong in.
        """

        rawList = os.listdir(sandboxDir)
        collections = []
        numberList = []
        for entry in rawList:
            if 'PackageCollection' in entry:
                collections.append(entry)

        # If we have no collections, return 0 (PackageCollection_0)
        if len(collections) < 1:
            return 0

        # Loop over the list of PackageCollections
        for collection in collections:
            collectionPath = os.path.join(sandboxDir, collection)
            packageList = os.listdir(collectionPath)
            collectionNum = int(collection.split('_')[1])
            if len(packageList) < self.collSize:
                return collectionNum
            else:
                numberList.append(collectionNum)

        # If we got here, then all collections are full.  We'll need
        # a new one.  Find the highest number, increment by one
        numberList.sort()
        return numberList[-1] + 1

    def addJobsToPackage(self, loadedJob):
        """
        _addJobsToPackage_

        Add a job to a job package and then return the batch ID for the job.
        Packages are only written out to disk when they contain 100 jobs.  The
        flushJobsPackages() method must be called after all jobs have been added
        to the cache and before they are actually submitted to make sure all the
        job packages have been written to disk.
        """
        if loadedJob["workflow"] not in self.jobsToPackage:
            # First, let's pull all the information from the loadedJob
            batchid = "%s-%s" % (loadedJob["id"], loadedJob["retry_count"])
            sandboxDir = os.path.dirname(loadedJob["sandbox"])

            # Second, assemble the jobPackage location
            collectionIndex = self.getPackageCollection(sandboxDir)
            collectionDir = os.path.join(sandboxDir,
                                         'PackageCollection_%i' % collectionIndex,
                                         'batch_%s' % batchid)

            # Now create the package object
            self.jobsToPackage[loadedJob["workflow"]] = {"batchid": batchid,
                                                         'id': loadedJob['id'],
                                                         "package": JobPackage(directory=collectionDir)}

        jobPackage = self.jobsToPackage[loadedJob["workflow"]]["package"]
        jobPackage[loadedJob["id"]] = loadedJob.getDataStructsJob()
        batchDir = jobPackage['directory']

        if len(jobPackage) == self.packageSize:
            if not os.path.exists(batchDir):
                os.makedirs(batchDir)

            batchPath = os.path.join(batchDir, "JobPackage.pkl")
            jobPackage.save(batchPath)
            del self.jobsToPackage[loadedJob["workflow"]]

        return batchDir

    def flushJobPackages(self):
        """
        _flushJobPackages_

        Write any jobs packages to disk that haven't been written out already.
        """
        workflowNames = list(self.jobsToPackage)
        for workflowName in workflowNames:
            jobPackage = self.jobsToPackage[workflowName]["package"]
            batchDir = jobPackage['directory']

            if not os.path.exists(batchDir):
                os.makedirs(batchDir)

            batchPath = os.path.join(batchDir, "JobPackage.pkl")
            jobPackage.save(batchPath)
            del self.jobsToPackage[workflowName]

        return

    def hasToRefreshCache(self):
        """
        _hasToRefreshCache_

        Check whether we should update the job data cache (or update it
        with new jobs in the created state) or if we just skip it.
        """
        if self.cacheRefreshSize == -1 or len(self.jobDataCache) < self.cacheRefreshSize or\
                        self.refreshPollingCount >= self.skipRefreshCount:
            self.refreshPollingCount = 0
            return True
        else:
            self.refreshPollingCount += 1
            logging.info("Skipping cache update to be submitted. (%s job in cache)", len(self.jobDataCache))
        return False

    def refreshCache(self):
        """
        _refreshCache_

        Query WMBS for all jobs in the 'created' state.  For all jobs returned
        from the query, check if they already exist in the cache.  If they
        don't, unpickle them and combine their site white and black list with
        the list of locations they can run at.  Add them to the cache.

        Each entry in the cache is a tuple with five items:
          - WMBS Job ID
          - Retry count
          - Batch ID
          - Path to sanbox
          - Path to cache directory
        """
        # make a counter for jobs pending to sites in drain mode within the grace period
        countDrainingJobs = 0
        timeNow = int(time.time())
        badJobs = dict([(x, []) for x in range(71101, 71106)])
        newJobIds = set()

        logging.info("Refreshing priority cache with currently %i jobs", len(self.jobDataCache))

        newJobs = self.listJobsAction.execute(limitRows=self.maxJobsToCache)
        if self.useReqMgrForCompletionCheck:
            # if reqmgr is used (not Tier0 Agent) get the aborted/forceCompleted record
            abortedAndForceCompleteRequests = self.abortedAndForceCompleteWorkflowCache.getData()
        else:
            abortedAndForceCompleteRequests = []

        logging.info("Found %s new jobs to be submitted.", len(newJobs))

        if self.enableAllSites:
            logging.info("Agent is in speed drain mode. Submitting jobs to all possible locations.")

        logging.info("Determining possible sites for new jobs...")
        jobCount = 0
        for newJob in newJobs:
            jobCount += 1
            if jobCount % 5000 == 0:
                logging.info("Processed %d/%d new jobs.", jobCount, len(newJobs))

            # whether newJob belongs to aborted or force-complete workflow, and skip it if it is.
            if newJob['request_name'] in abortedAndForceCompleteRequests and \
                            newJob['task_type'] not in ['LogCollect', "Cleanup"]:
                continue

            jobID = newJob['id']
            newJobIds.add(jobID)
            if jobID in self.jobDataCache:
                continue

            pickledJobPath = os.path.join(newJob["cache_dir"], "job.pkl")

            if not os.path.isfile(pickledJobPath):
                # Then we have a problem - there's no file
                logging.warning("Could not find pickled jobObject %s", pickledJobPath)
                badJobs[71104].append(newJob)
                continue
            try:
                with open(pickledJobPath, 'rb') as jobHandle:
                    loadedJob = pickle.load(jobHandle)
            except Exception as ex:
                logging.warning("Failed to load job pickle object %s", pickledJobPath)
                badJobs[71105].append(newJob)
                continue

            # figure out possible locations for job
            possibleLocations = loadedJob["possiblePSN"]

            # Create another set of locations that may change when a site goes white/black listed
            # Does not care about the non_draining or aborted sites, they may change and that is the point
            potentialLocations = set()
            potentialLocations.update(possibleLocations)

            # check if there is at least one site left to run the job
            if len(possibleLocations) == 0:
                newJob['fileLocations'] = loadedJob.get('fileLocations', [])
                newJob['siteWhitelist'] = loadedJob.get('siteWhitelist', [])
                newJob['siteBlacklist'] = loadedJob.get('siteBlacklist', [])
                logging.warning("Input data location doesn't pass the site restrictions for job id: %s", jobID)
                badJobs[71101].append(newJob)
                continue

            # if agent is in speed drain and has hit the threshold to submit to all sites, we can skip the logic below that exclude sites
            if not self.enableAllSites:
                # check for sites in aborted state and adjust the possible locations
                nonAbortSites = [x for x in possibleLocations if x not in self.abortSites]
                if nonAbortSites:  # if there is at least a non aborted/down site then run there, otherwise fail the job
                    possibleLocations = nonAbortSites
                else:
                    newJob['possibleSites'] = possibleLocations
                    logging.warning("Job id %s can only run at a site in Aborted state", jobID)
                    badJobs[71102].append(newJob)
                    continue

                # try to remove draining sites if possible, this is needed to stop
                # jobs that could run anywhere blocking draining sites
                # if the job type is Merge, LogCollect or Cleanup this is skipped
                if newJob['task_type'] not in self.ioboundTypes:
                    nonDrainingSites = [x for x in possibleLocations if x not in self.drainSites]
                    if nonDrainingSites:  # if >1 viable non-draining site remove draining ones
                        possibleLocations = nonDrainingSites
                    elif self.failJobDrain(timeNow, possibleLocations):
                        newJob['possibleSites'] = possibleLocations
                        logging.warning("Job id %s can only run at a sites in Draining state", jobID)
                        badJobs[71103].append(newJob)
                        continue
                    else:
                        countDrainingJobs += 1
                        continue

            # Sigh...make sure the job added to the package has the proper retry_count
            loadedJob['retry_count'] = newJob['retry_count']
            batchDir = self.addJobsToPackage(loadedJob)

            # calculate the final job priority such that we can order cached jobs by prio
            jobPrio = newJob['task_prio'] * self.maxTaskPriority + newJob['wf_priority']
            self.jobsByPrio.setdefault(jobPrio, set())
            self.jobsByPrio[jobPrio].add(jobID)

            # allow job baggage to override numberOfCores
            #       => used for repacking to get more slots/disk
            numberOfCores = loadedJob.get('numberOfCores', 1)
            if numberOfCores == 1:
                baggage = loadedJob.getBaggage()
                numberOfCores = getattr(baggage, "numberOfCores", 1)
            loadedJob['numberOfCores'] = numberOfCores

            # Create a job dictionary object and put it in the cache (needs to be in sync with RunJob)
            jobInfo = {'taskPriority': newJob['task_prio'],
                       'activity': loadedJob.get("taskType"),
                       'custom': {'location': None},  # update later
                       'packageDir': batchDir,
                       'retry_count': newJob["retry_count"],
                       'sandbox': loadedJob["sandbox"],  # remove before submit
                       'userdn': loadedJob.get("ownerDN", None),
                       'usergroup': loadedJob.get("ownerGroup", ''),
                       'userrole': loadedJob.get("ownerRole", ''),
                       'possibleSites': frozenset(possibleLocations),  # abort and drain sites filtered out
                       'potentialSites': frozenset(potentialLocations),  # original list of sites
                       'scramArch': loadedJob.get("scramArch", None),
                       'swVersion': loadedJob.get("swVersion", []),
                       'proxyPath': loadedJob.get("proxyPath", None),
                       'estimatedJobTime': loadedJob.get("estimatedJobTime", None),
                       'estimatedDiskUsage': loadedJob.get("estimatedDiskUsage", None),
                       'estimatedMemoryUsage': loadedJob.get("estimatedMemoryUsage", None),
                       'numberOfCores': loadedJob.get("numberOfCores"),  # may update it later
                       'inputDataset': loadedJob.get('inputDataset', None),
                       'inputDatasetLocations': loadedJob.get('inputDatasetLocations', None),
                       'inputPileup': loadedJob.get('inputPileup', None),
                       'allowOpportunistic': loadedJob.get('allowOpportunistic', False),
                       'requiresGPU': loadedJob.get('requiresGPU', "forbidden"),
                       'gpuRequirements': loadedJob.get('gpuRequirements', None),
                       'requestType': loadedJob['requestType'],
                       }
            # then update it with the info retrieved from the database
            jobInfo.update(newJob)

            self.jobDataCache[jobID] = jobInfo

        # Register failures in submission
        for errorCode in badJobs:
            if badJobs[errorCode] and errorCode in [71101, 71102, 71103]:
                msg = "%d jobs failed to be submitted due to location constraints (error code: %s)"
                logging.warning(msg, len(badJobs[errorCode]), errorCode)
                self._handleSubmitFailedJobs(badJobs[errorCode], errorCode)
            elif badJobs[errorCode] and errorCode in [71104, 71105]:
                msg = "%d jobs failed to be submitted with unrecoverable job pickle problems (error code: %s)"
                logging.warning(msg, len(badJobs[errorCode]), errorCode)
                self._handleSubmitFailedJobs(badJobs[errorCode], errorCode)

        # Persist remaining job packages to disk
        self.flushJobPackages()

        # We need to remove any jobs from the cache that were not returned in
        # the last call to the database.
        jobIDsToPurge = set(self.jobDataCache.keys()) - newJobIds
        self._purgeJobsFromCache(jobIDsToPurge)

        logging.info("Found %d jobs pending to sites in drain within the grace period", countDrainingJobs)
        logging.info("Done pruning killed jobs, moving on to submit.")
        return

    def failJobDrain(self, timeNow, possibleLocations):
        """
        Check whether sites are in drain for too long such that the job
        has to be marked as failed or not.
        :param timeNow: timestamp for this cycle
        :param possibleLocations: list of possible locations where the job can run
        :return: a boolean saying whether the job has to fail or not
        """
        fail = True
        for siteName in set(possibleLocations).union(self.drainSitesSet):
            if timeNow - self.drainSites[siteName] < self.drainGracePeriod:
                # then let this job be, it's a fresh draining site
                fail = False
                break
        return fail

    def removeAbortedForceCompletedWorkflowFromCache(self):
        abortedAndForceCompleteRequests = self.abortedAndForceCompleteWorkflowCache.getData()
        jobIDsToPurge = set()
        for jobID, jobInfo in viewitems(self.jobDataCache):
            if (jobInfo['request_name'] in abortedAndForceCompleteRequests) and \
                    (jobInfo['task_type'] not in ['LogCollect', "Cleanup"]):
                jobIDsToPurge.add(jobID)
        self._purgeJobsFromCache(jobIDsToPurge)
        return

    def _purgeJobsFromCache(self, jobIDsToPurge):

        if len(jobIDsToPurge) == 0:
            return

        for jobid in jobIDsToPurge:
            self.jobDataCache.pop(jobid, None)
            for jobPrio in self.jobsByPrio:
                if jobid in self.jobsByPrio[jobPrio]:
                    # then the jobid was found, go to the next one
                    self.jobsByPrio[jobPrio].discard(jobid)
                    break
        return

    def _handleSubmitFailedJobs(self, badJobs, exitCode):
        """
        __handleSubmitFailedJobs_

        For a default job report for the exitCode
        and register in the job. Preserve it on disk as well.
        Propagate the failure to the JobStateMachine.
        """
        fwjrBinds = []
        for job in badJobs:
            job['couch_record'] = None
            job['fwjr'] = Report()
            if exitCode in [71102, 71103]:
                job['fwjr'].addError("JobSubmit", exitCode, "SubmitFailed",
                                     WM_JOB_ERROR_CODES[exitCode] + ', '.join(job['possibleSites']),
                                     ', '.join(job['possibleSites']))
            elif exitCode in [71101]:
                # there is no possible site
                if job.get("fileLocations"):
                    job['fwjr'].addError("JobSubmit", exitCode, "SubmitFailed", WM_JOB_ERROR_CODES[exitCode] +
                                         ": file locations: " + ', '.join(job['fileLocations']) +
                                         ": site white list: " + ', '.join(job['siteWhitelist']) +
                                         ": site black list: " + ', '.join(job['siteBlacklist']))
                else:
                    job['fwjr'].addError("JobSubmit", exitCode, "SubmitFailed",
                                         WM_JOB_ERROR_CODES[exitCode] + ', and empty fileLocations')

            else:
                job['fwjr'].addError("JobSubmit", exitCode, "SubmitFailed", WM_JOB_ERROR_CODES[exitCode])

            fwjrPath = os.path.join(job['cache_dir'], 'Report.%d.pkl' % int(job['retry_count']))
            job['fwjr'].setJobID(job['id'])
            try:
                job['fwjr'].save(fwjrPath)
                fwjrBinds.append({"jobid": job["id"], "fwjrpath": fwjrPath})
            except IOError as ioer:
                logging.error("Failed to write FWJR for submit failed job %d, message: %s", job['id'], str(ioer))
        self.changeState.propagate(badJobs, "submitfailed", "created")
        self.setFWJRPathAction.execute(binds=fwjrBinds)
        return

    def getThresholds(self):
        """
        _getThresholds_

        Retrieve submit thresholds, which considers what is pending and running
        for those sites.
        Also update the list of draining and abort/down sites.
        Finally, creates a map between task type and its priority.
        """
        # lets store also a timestamp for when a site joined the Drain state
        newDrainSites = dict()
        newAbortSites = set()

        rcThresholds = self.resourceControl.listThresholdsForSubmit()

        for siteName in rcThresholds:
            # Add threshold if we don't have it already
            state = rcThresholds[siteName]["state"]

            if state == "Draining":
                newDrainSites.update({siteName: rcThresholds[siteName]["state_time"]})
            if state in ["Down", "Aborted"]:
                newAbortSites.add(siteName)

        # When the list of drain/abort sites change between iteration then a location
        # refresh is needed, for now it forces a full cache refresh
        if set(newDrainSites.keys()) != self.drainSitesSet or newAbortSites != self.abortSites:
            logging.info("Draining or Aborted sites have changed, the cache will be rebuilt.")
            self.jobsByPrio = {}
            self.jobDataCache = {}

        self.currentRcThresholds = rcThresholds
        self.abortSites = newAbortSites
        self.drainSites = newDrainSites
        self.drainSitesSet = set(newDrainSites.keys())

        return

    def checkZeroTaskThresholds(self, jobType, siteList):
        """
        _checkZeroTaskThresholds_

        Given a job type and a list of sites, remove sites from the list
        if that site + task has 0 pending thresholds.
        Returns a new list of sites
        """
        newSiteList = []
        for site in siteList:
            try:
                taskPendingSlots = self.currentRcThresholds[site]['thresholds'][jobType]["pending_slots"]
            except KeyError as ex:
                msg = "Invalid key for site %s and job type %s. Error: %s" % (site, jobType, str(ex))
                logging.warning(msg)
            else:
                if taskPendingSlots > 0:
                    newSiteList.append(site)

        return newSiteList

    def _getJobSubmitCondition(self, jobPrio, siteName, jobType):
        """
        returns the string describing whether a job is ready to be submitted or the reason can't be submitted
        Only jobs with "JobSubmitReady" return value will be added to submit job.
        Other return values will indicate the reason jobs cannot be submitted.
        i.e. "NoPendingSlot"  - pending slot is full with pending job
        """
        try:
            totalPendingSlots = self.currentRcThresholds[siteName]["total_pending_slots"]
            totalPendingJobs = self.currentRcThresholds[siteName]["total_pending_jobs"]
            totalRunningSlots = self.currentRcThresholds[siteName]["total_running_slots"]
            totalRunningJobs = self.currentRcThresholds[siteName]["total_running_jobs"]

            taskPendingSlots = self.currentRcThresholds[siteName]['thresholds'][jobType]["pending_slots"]
            taskPendingJobs = self.currentRcThresholds[siteName]['thresholds'][jobType]["task_pending_jobs"]
            taskRunningSlots = self.currentRcThresholds[siteName]['thresholds'][jobType]["max_slots"]
            taskRunningJobs = self.currentRcThresholds[siteName]['thresholds'][jobType]["task_running_jobs"]
            highestPriorityInJobs = self.currentRcThresholds[siteName]['thresholds'][jobType]['wf_highest_priority']

            # set the initial totalPendingJobs since it increases in every cycle when a job is submitted
            self.currentRcThresholds[siteName].setdefault("init_total_pending_jobs", totalPendingJobs)

            # set the initial taskPendingJobs since it increases in every cycle when a job is submitted
            self.currentRcThresholds[siteName]['thresholds'][jobType].setdefault("init_task_pending_jobs",
                                                                                 taskPendingJobs)

            initTotalPending = self.currentRcThresholds[siteName]["init_total_pending_jobs"]
            initTaskPending = self.currentRcThresholds[siteName]['thresholds'][jobType]["init_task_pending_jobs"]

        except KeyError as ex:
            msg = "Invalid key for site %s and job type %s\n" % (siteName, jobType)
            msg += str(ex)
            logging.exception(msg)
            return "NoJobType_%s_%s" % (siteName, jobType)

        if (highestPriorityInJobs is None) or (jobPrio <= highestPriorityInJobs) or (jobType in self.ioboundTypes):
            # there is no pending or running jobs in the system (None case) or
            # priority of the job is lower or equal don't allow overflow
            # Also if jobType is in ioboundTypes don't allow overflow
            totalPendingThreshold = totalPendingSlots
            taskPendingThreshold = taskPendingSlots
            totalJobThreshold = totalPendingSlots + totalRunningSlots
            totalTaskTheshold = taskPendingSlots + taskRunningSlots
        else:
            # In case the priority of the job is higher than any of currently pending or running jobs.
            # Then increase the threshold by condorOverflowFraction * original pending slot.
            totalPendingThreshold = max(totalPendingSlots, initTotalPending) + (
                totalPendingSlots * self.condorOverflowFraction)
            taskPendingThreshold = max(taskPendingSlots, initTaskPending) + (
                taskPendingSlots * self.condorOverflowFraction)
            totalJobThreshold = totalPendingThreshold + totalRunningSlots
            totalTaskTheshold = taskPendingThreshold + taskRunningSlots

        jobStats = [{"Condition": "NoPendingSlot",
                     "Current": totalPendingJobs,
                     "Threshold": totalPendingThreshold},
                    {"Condition": "NoTaskPendingSlot",
                     "Current": taskPendingJobs,
                     "Threshold": taskPendingThreshold},
                    {"Condition": "NoRunningSlot",
                     "Current": totalPendingJobs + totalRunningJobs,
                     "Threshold": totalJobThreshold},
                    {"Condition": "NoTaskRunningSlot",
                     "Current": taskPendingJobs + taskRunningJobs,
                     "Threshold": totalTaskTheshold}]
        return jobSubmitCondition(jobStats)

    def assignJobLocations(self):
        """
        _assignJobLocations_

        Loop through the submit thresholds and pull sites out of the job cache
        as we discover open slots.  This will return a list of tuple where each
        tuple will have six elements:
          - WMBS Job ID
          - Retry count
          - Batch ID
          - Path to sanbox
          - Path to cache directory
          - SE name of the site to run at
        """
        jobsToSubmit = {}
        jobsCount = 0
        exitLoop = False
        jobSubmitLogBySites = defaultdict(lambda: defaultdict(Counter))
        jobSubmitLogByPriority = defaultdict(lambda: defaultdict(Counter))

        # iterate over jobs from the highest to the lowest prio
        for jobPrio in sorted(self.jobsByPrio, reverse=True):

            # then we're completely done and have our basket full of jobs to submit
            if exitLoop:
                break

            # can we assume jobid=1 is older than jobid=3? I think so...
            for jobid in sorted(self.jobsByPrio[jobPrio]):
                jobType = self.jobDataCache[jobid]['task_type']
                possibleSites = self.jobDataCache[jobid]['possibleSites']
                # remove sites with 0 task thresholds
                possibleSites = self.checkZeroTaskThresholds(jobType, possibleSites)
                jobSubmitLogByPriority[jobPrio][jobType]['Total'] += 1
                # now look for sites with free pending slots
                for siteName in possibleSites:
                    condition = self._getJobSubmitCondition(jobPrio, siteName, jobType)
                    if condition != "JobSubmitReady":
                        jobSubmitLogBySites[siteName][jobType][condition] += 1
                        logging.debug("Found a job for %s : %s", siteName, condition)
                        continue

                    # pop the job dictionary object and update it
                    cachedJob = self.jobDataCache.pop(jobid)
                    cachedJob['custom'] = {'location': siteName}
                    cachedJob['possibleSites'] = possibleSites

                    # Sort jobs by jobPackage and get it in place to be submitted by the plugin
                    package = cachedJob['packageDir']
                    jobsToSubmit.setdefault(package, [])
                    jobsToSubmit[package].append(cachedJob)

                    # update site/task thresholds and the component job counter
                    self.currentRcThresholds[siteName]["total_pending_jobs"] += 1
                    self.currentRcThresholds[siteName]['thresholds'][jobType]["task_pending_jobs"] += 1
                    jobsCount += 1
                    jobSubmitLogBySites[siteName][jobType]["submitted"] += 1
                    jobSubmitLogByPriority[jobPrio][jobType]['submitted'] += 1

                    # jobs that will be submitted must leave the job data cache
                    self.jobsByPrio[jobPrio].discard(jobid)

                    # found a site to submit this job, so go to the next job
                    break

                # set the flag and get out of the job iteration
                if jobsCount >= self.maxJobsThisCycle:
                    logging.info("Submitter reached limit of submit slots for this cycle: %i", self.maxJobsThisCycle)
                    exitLoop = True
                    break

        logging.info("Site submission report ...")
        for site in jobSubmitLogBySites:
            logging.info("    %s : %s", site, json.dumps(jobSubmitLogBySites[site]))
        logging.info("Priority submission report ...")
        for prio in jobSubmitLogByPriority:
            logging.info("    %s : %s", prio, json.dumps(jobSubmitLogByPriority[prio]))
        logging.info("Have %s packages to submit.", len(jobsToSubmit))
        logging.info("Have %s jobs to submit.", jobsCount)
        logging.info("Done assigning site locations.")
        return jobsToSubmit

    def submitJobs(self, jobsToSubmit):
        """
        _submitJobs_

        Actually do the submission of the jobs
        """

        jobList = []
        idList = []

        if len(jobsToSubmit) == 0:
            logging.debug("There are no packages to submit.")
            return

        for package in jobsToSubmit:
            jobs = jobsToSubmit.get(package, [])
            for job in jobs:
                job['location'], job['plugin'], job['site_cms_name'] = self.getSiteInfo(job['custom']['location'])
                idList.append({'jobid': job['id'], 'location': job['custom']['location']})

            jobList.extend(jobs)

        myThread = threading.currentThread()
        myThread.transaction.begin()

        # Run the actual underlying submit code using bossAir
        successList, failList = self.bossAir.submit(jobs=jobList)
        logging.info("Jobs that succeeded/failed submission: %d/%d.", len(successList), len(failList))

        # Propagate states in the WMBS database
        logging.debug("Propagating success state to WMBS.")
        self.changeState.propagate(successList, 'executing', 'created')
        logging.debug("Propagating fail state to WMBS.")
        self.changeState.propagate(failList, 'submitfailed', 'created')
        # At the end we mark the locations of the jobs
        # This applies even to failed jobs, since the location
        # could be part of the failure reason.
        logging.debug("Updating job location...")
        self.setLocationAction.execute(bulkList=idList, conn=myThread.transaction.conn,
                                       transaction=True)
        myThread.transaction.commit()
        logging.info("Transaction cycle successfully completed.")

        return

    def getSiteInfo(self, jobSite):
        """
        _getSiteInfo_

        This is how you get the name of a CE and the plugin for a job
        """

        if jobSite not in self.locationDict:
            siteInfo = self.locationAction.execute(siteName=jobSite)
            self.locationDict[jobSite] = siteInfo[0]
        return (self.locationDict[jobSite].get('ce_name'),
                self.locationDict[jobSite].get('plugin'),
                self.locationDict[jobSite].get('cms_name'))

    @timeFunction
    def algorithm(self, parameters=None):
        """
        _algorithm_

        Try to, in order:
        1) Refresh the cache
        2) Find jobs for all the necessary sites
        3) Submit the jobs to the plugin
        """
        myThread = threading.currentThread()

        if self.useReqMgrForCompletionCheck:
            # only runs when reqmgr is used (not Tier0)
            self.removeAbortedForceCompletedWorkflowFromCache()
            agentConfig = self.reqAuxDB.getWMAgentConfig(self.config.Agent.hostName)
            if agentConfig.get("UserDrainMode") and agentConfig.get("SpeedDrainMode"):
                self.enableAllSites = agentConfig.get("SpeedDrainConfig")['EnableAllSites']['Enabled']
            else:
                self.enableAllSites = False
            self.condorFraction = agentConfig.get('CondorJobsFraction', 0.75)
            self.condorOverflowFraction = agentConfig.get("CondorOverflowFraction", 0.2)
        else:
            # For Tier0 agent
            self.condorFraction = 1
            self.condorOverflowFraction = 0

        if not self.passSubmitConditions():
            msg = "JobSubmitter didn't pass the submit conditions. Skipping this cycle."
            logging.warning(msg)
            myThread.logdbClient.post("JobSubmitter_submitWork", msg, "warning")
            return

        try:
            myThread.logdbClient.delete("JobSubmitter_submitWork", "warning", this_thread=True)
            self.getThresholds()
            if self.hasToRefreshCache():
                self.refreshCache()

            jobsToSubmit = self.assignJobLocations()
            self.submitJobs(jobsToSubmit=jobsToSubmit)
        except WMException:
            if getattr(myThread, 'transaction', None) is not None:
                myThread.transaction.rollback()
            raise
        except Exception as ex:
            msg = 'Fatal error in JobSubmitter:\n'
            msg += str(ex)
            # msg += str(traceback.format_exc())
            msg += '\n\n'
            logging.error(msg)
            if getattr(myThread, 'transaction', None) is not None:
                myThread.transaction.rollback()
            raise JobSubmitterPollerException(msg)

        return

    def passSubmitConditions(self):
        """
        _passSubmitConditions_

        Check whether the component is allowed to submit jobs to condor.

        Initially it has only one condition, which is the total number of
        jobs we can have in condor (pending + running) per schedd, set by
        MAX_JOBS_PER_OWNER.
        """

        myThread = threading.currentThread()
        freeSubmitSlots = availableScheddSlots(dbi=myThread.dbi, logger=logging,
                                               condorFraction=self.condorFraction)
        self.maxJobsThisCycle = min(freeSubmitSlots, self.maxJobsPerPoll)

        return (self.maxJobsThisCycle > 0)

    def terminate(self, params):
        """
        _terminate_

        Kill the code after one final pass when called by the master thread.
        """
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)
