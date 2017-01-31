#!/usr/bin/env python
#pylint: disable=C0301
# for the whitelist and the blacklist
# C0301: I'm ignoring this because breaking up error messages is painful
"""
_JobSubmitterPoller_t_

Submit jobs for execution.
"""

import logging
import threading
import os.path
from collections import defaultdict, Counter
from operator import itemgetter
try:
    import cPickle as pickle
except ImportError:
    import pickle

from WMCore.DAOFactory        import DAOFactory
from WMCore.WMExceptions      import WM_JOB_ERROR_CODES

from WMCore.JobStateMachine.ChangeState       import ChangeState
from WMCore.WorkerThreads.BaseWorkerThread    import BaseWorkerThread
from WMCore.ResourceControl.ResourceControl   import ResourceControl
from WMCore.DataStructs.JobPackage            import JobPackage
from WMCore.FwkJobReport.Report               import Report
from WMCore.WMException                       import WMException
from WMCore.BossAir.BossAirAPI                import BossAirAPI
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr


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

        #DAO factory for WMBS objects
        self.daoFactory = DAOFactory(package="WMCore.WMBS", logger=logging, dbinterface=myThread.dbi)

        #Libraries
        self.resourceControl = ResourceControl()
        self.changeState = ChangeState(self.config)
        self.bossAir = BossAirAPI(config=self.config)

        self.repollCount = getattr(self.config.JobSubmitter, 'repollCount', 10000)
        self.maxJobsPerPoll = int(getattr(self.config.JobSubmitter, 'maxJobsPerPoll', 1000))
        self.cacheRefreshSize = int(getattr(self.config.JobSubmitter, 'cacheRefreshSize', 30000))
        self.skipRefreshCount = int(getattr(self.config.JobSubmitter, 'skipRefreshCount', 20))
        self.packageSize = getattr(self.config.JobSubmitter, 'packageSize', 500)
        self.collSize = getattr(self.config.JobSubmitter, 'collectionSize', self.packageSize * 1000)
        self.maxTaskPriority = getattr(self.config.BossAir, 'maxTaskPriority', 1e7)

        # Additions for caching-based JobSubmitter
        self.cachedJobIDs = set()
        self.cachedJobs = {}
        self.jobDataCache = {}
        self.jobsToPackage = {}
        self.sandboxPackage = {}
        self.locationDict = {}
        self.taskTypePrioMap = {}
        self.drainSites = set()
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
            self.reqmgr2Svc = ReqMgr(self.config.TaskArchiver.ReqMgr2ServiceURL)
            self.abortedAndForceCompleteWorkflowCache = self.reqmgr2Svc.getAbortedAndForceCompleteRequestsFromMemoryCache()
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

        if len(jobPackage.keys()) == self.packageSize:
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
        workflowNames = self.jobsToPackage.keys()
        for workflowName in workflowNames:
            jobPackage = self.jobsToPackage[workflowName]["package"]
            batchDir = jobPackage['directory']

            if not os.path.exists(batchDir):
                os.makedirs(batchDir)

            batchPath = os.path.join(batchDir, "JobPackage.pkl")
            jobPackage.save(batchPath)
            del self.jobsToPackage[workflowName]

        return

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
        badJobs = dict([(x, []) for x in range(71101, 71105)])
        dbJobs = set()

        logging.info("Refreshing priority cache with currently %i jobs", len(self.cachedJobIDs))

        if self.cacheRefreshSize == -1 or len(self.cachedJobIDs) < self.cacheRefreshSize or \
           self.refreshPollingCount >= self.skipRefreshCount:
            newJobs = self.listJobsAction.execute()
            self.refreshPollingCount = 0
            
            if self.useReqMgrForCompletionCheck:
                # if reqmgr is used (not Tier0 Agent) get the aborted/forceCompleted record
                abortedAndForceCompleteRequests = self.abortedAndForceCompleteWorkflowCache.getData()
            else:
                #T0Agent
                abortedAndForceCompleteRequests = []
                
            logging.info("Found %s new jobs to be submitted.", len(newJobs))
        else:
            self.refreshPollingCount += 1
            newJobs = []
            dbJobs = self.cachedJobIDs
            abortedAndForceCompleteRequests = []
            logging.info("Skipping cache update to be submitted. (%s job in cache)", len(dbJobs))

        logging.info("Determining possible sites for new jobs...")
        jobCount = 0
        for newJob in newJobs:
            # whether newJob belongs to aborted or force-complete workflow, and skip it if it is.
            if (newJob['request_name'] in abortedAndForceCompleteRequests) and \
               (newJob['type'] not in ['LogCollect', "Cleanup"]):
                continue

            jobID = newJob['id']
            dbJobs.add(jobID)
            if jobID in self.cachedJobIDs:
                continue

            jobCount += 1
            if jobCount % 5000 == 0:
                logging.info("Processed %d/%d new jobs.", jobCount, len(newJobs))

            pickledJobPath = os.path.join(newJob["cache_dir"], "job.pkl")

            if not os.path.isfile(pickledJobPath):
                # Then we have a problem - there's no file
                logging.error("Could not find pickled jobObject %s", pickledJobPath)
                badJobs[71103].append(newJob)
                continue
            try:
                jobHandle = open(pickledJobPath, "r")
                loadedJob = pickle.load(jobHandle)
                jobHandle.close()
            except Exception as ex:
                msg = "Error while loading pickled job object %s\n" % pickledJobPath
                msg += str(ex)
                logging.error(msg)
                raise JobSubmitterPollerException(msg)

            loadedJob['retry_count'] = newJob['retry_count']

            # figure out possible locations for job
            possibleLocations = loadedJob["possiblePSN"]

            # Create another set of locations that may change when a site goes white/black listed
            # Does not care about the non_draining or aborted sites, they may change and that is the point
            potentialLocations = set()
            potentialLocations.update(possibleLocations)

            # now check for sites in drain and adjust the possible locations
            # also check if there is at least one site left to run the job
            if len(possibleLocations) == 0:
                newJob['name'] = loadedJob['name']
                newJob['fileLocations'] = loadedJob.get('fileLocations', [])
                newJob['siteWhitelist'] = loadedJob.get('siteWhitelist', [])
                newJob['siteBlacklist'] = loadedJob.get('siteBlacklist', [])
                badJobs[71101].append(newJob)
                continue
            else:
                nonAbortSites = [x for x in possibleLocations if x not in self.abortSites]
                if nonAbortSites: # if there is at least a non aborted/down site then run there, otherwise fail the job
                    possibleLocations = nonAbortSites
                else:
                    newJob['name'] = loadedJob['name']
                    newJob['possibleLocations'] = possibleLocations
                    badJobs[71102].append(newJob)
                    continue

            # try to remove draining sites if possible, this is needed to stop
            # jobs that could run anywhere blocking draining sites
            # if the job type is Merge, LogCollect or Cleanup this is skipped
            if newJob['type'] not in ('LogCollect', 'Merge', 'Cleanup', 'Harvesting'):
                nonDrainingSites = [x for x in possibleLocations if x not in self.drainSites]
                if nonDrainingSites: # if >1 viable non-draining site remove draining ones
                    possibleLocations = nonDrainingSites
                else:
                    newJob['name'] = loadedJob['name']
                    newJob['possibleLocations'] = possibleLocations
                    badJobs[71104].append(newJob)
                    continue

            # locations clear of abort and draining sites
            newJob['possibleLocations'] = possibleLocations

            batchDir = self.addJobsToPackage(loadedJob)
            self.cachedJobIDs.add(jobID)

            # calculate the final job priority such that we can order cached jobs by prio
            jobPrio = self.taskTypePrioMap.get(newJob['type'], 0) + newJob['wf_priority']
            if jobPrio not in self.cachedJobs:
                self.cachedJobs[jobPrio] = {}

            # now add basic information keyed by the jobid
            self.cachedJobs[jobPrio][jobID] = newJob

            # allow job baggage to override numberOfCores
            #       => used for repacking to get more slots/disk
            numberOfCores = loadedJob.get('numberOfCores', 1)
            if numberOfCores == 1:
                baggage = loadedJob.getBaggage()
                numberOfCores = getattr(baggage, "numberOfCores", 1)
            loadedJob['numberOfCores'] = numberOfCores

            # Create a job dictionary object and put it in the cache (needs to be in sync with RunJob)
            jobInfo = {'id': jobID,
                       'requestName': newJob['request_name'],
                       'taskName': newJob['task_name'],
                       'taskType': newJob['type'],
                       'cache_dir': newJob["cache_dir"],
                       'priority': newJob['wf_priority'],
                       'taskID': newJob['task_id'],
                       'retry_count': newJob["retry_count"],
                       'taskPriority': None,                                # update from the thresholds
                       'custom': {'location': None},                        # update later
                       'packageDir': batchDir,
                       'sandbox': loadedJob["sandbox"],                     # remove before submit
                       'userdn': loadedJob.get("ownerDN", None),
                       'usergroup': loadedJob.get("ownerGroup", ''),
                       'userrole': loadedJob.get("ownerRole", ''),
                       'possibleSites': frozenset(possibleLocations),       # abort and drain sites filtered out
                       'potentialSites': frozenset(potentialLocations),     # original list of sites
                       'scramArch': loadedJob.get("scramArch", None),
                       'swVersion': loadedJob.get("swVersion", None),
                       'name': loadedJob["name"],
                       'proxyPath': loadedJob.get("proxyPath", None),
                       'estimatedJobTime': loadedJob.get("estimatedJobTime", None),
                       'estimatedDiskUsage': loadedJob.get("estimatedDiskUsage", None),
                       'estimatedMemoryUsage': loadedJob.get("estimatedMemoryUsage", None),
                       'numberOfCores': loadedJob.get("numberOfCores", 1),  # may update it later
                       'inputDataset': loadedJob.get('inputDataset', None),
                       'inputDatasetLocations': loadedJob.get('inputDatasetLocations', None),
                       'allowOpportunistic': loadedJob.get('allowOpportunistic', False)}

            self.jobDataCache[jobID] = jobInfo

        # Register failures in submission
        for errorCode in badJobs:
            if badJobs[errorCode]:
                logging.debug("The following jobs could not be submitted: %s, error code : %d", badJobs, errorCode)
                self._handleSubmitFailedJobs(badJobs[errorCode], errorCode)

        # If there are any leftover jobs, we want to get rid of them.
        self.flushJobPackages()

        # We need to remove any jobs from the cache that were not returned in
        # the last call to the database.
        jobIDsToPurge = self.cachedJobIDs - dbJobs
        self._purgeJobsFromCache(jobIDsToPurge)

        logging.info("Done pruning killed jobs, moving on to submit.")
        return
 
    def removeAbortedForceCompletedWorkflowFromCache(self):
        abortedAndForceCompleteRequests = self.abortedAndForceCompleteWorkflowCache.getData()
        jobIDsToPurge = set() 
        for jobID, jobInfo in self.jobDataCache.iteritems():
            if (jobInfo['requestName'] in abortedAndForceCompleteRequests) and \
               (jobInfo['taskType'] not in ['LogCollect', "Cleanup"]):
                jobIDsToPurge.add(jobID)
        self._purgeJobsFromCache(jobIDsToPurge)
        return
    
    def _purgeJobsFromCache(self, jobIDsToPurge):
        
        if len(jobIDsToPurge) == 0:
            return
        
        self.cachedJobIDs -= jobIDsToPurge

        for jobid in jobIDsToPurge:
            self.jobDataCache.pop(jobid, None)
            for jobPrio in self.cachedJobs:
                if self.cachedJobs[jobPrio].pop(jobid, None):
                    # then the jobid was found, go to the next one
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
            if exitCode in [71102, 71104]:
                job['fwjr'].addError("JobSubmit", exitCode, "SubmitFailed", WM_JOB_ERROR_CODES[exitCode] + ', '.join(job['possibleLocations']))
            elif exitCode in [71101]:
                # there is no possible site 
                if job.get("fileLocations"):
                    job['fwjr'].addError("JobSubmit", exitCode, "SubmitFailed", WM_JOB_ERROR_CODES[exitCode]  + 
                                         ": file locations: " + ', '.join(job['fileLocations']) +
                                         ": site white list: " + ', '.join(job['siteWhitelist']) +
                                         ": site black list: " + ', '.join(job['siteBlacklist']))
                else:
                    # This is temporary addition if this is patched for existing agent.
                    # If jobs are created before the patch is applied fileLocations is not set.
                    # TODO. remove this later for new agent
                    job['fwjr'].addError("JobSubmit", exitCode, "SubmitFailed", WM_JOB_ERROR_CODES[exitCode]  + 
                                         ": Job is created before this patch. Please check this input for the jobs: %s " % 
                                         job['fwjr'].getAllInputFiles())
                    
            else:
                job['fwjr'].addError("JobSubmit", exitCode, "SubmitFailed", WM_JOB_ERROR_CODES[exitCode])
            fwjrPath = os.path.join(job['cache_dir'],
                                    'Report.%d.pkl' % int(job['retry_count']))
            job['fwjr'].setJobID(job['id'])
            try:
                job['fwjr'].save(fwjrPath)
                fwjrBinds.append({"jobid" : job["id"], "fwjrpath" : fwjrPath})
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
        self.taskTypePrioMap = {}
        newDrainSites = set()
        newAbortSites = set()

        rcThresholds = self.resourceControl.listThresholdsForSubmit()

        for siteName in rcThresholds.keys():
            # Add threshold if we don't have it already
            state = rcThresholds[siteName]["state"]

            if state == "Draining":
                newDrainSites.add(siteName)
            if state in ["Down", "Aborted"]:
                newAbortSites.add(siteName)

            # then update the task type x task priority mapping
            if not self.taskTypePrioMap:
                for task, value in rcThresholds[siteName]['thresholds'].items():
                    self.taskTypePrioMap[task] = value.get('priority', 0) * self.maxTaskPriority

        # When the list of drain/abort sites change between iteration then a location
        # refresh is needed, for now it forces a full cache refresh
        if newDrainSites != self.drainSites or  newAbortSites != self.abortSites:
            logging.info("Draining or Aborted sites have changed, the cache will be rebuilt.")
            self.cachedJobIDs = set()
            self.cachedJobs = {}
            self.jobDataCache = {}

        self.currentRcThresholds = rcThresholds
        self.abortSites = newAbortSites
        self.drainSites = newDrainSites

        return

        
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
        jobsToUncache = []
        jobsCount = 0
        exitLoop = False
        jobSubmitLogBySites = defaultdict(Counter)
        jobSubmitLogByPriority = defaultdict(Counter)

        # iterate over jobs from the highest to the lowest prio
        for jobPrio in sorted(self.cachedJobs, reverse=True):

            # then we're completely done and have our basket full of jobs to submit
            if exitLoop:
                break

            # start eating through the elder jobs first
            for job in sorted(self.cachedJobs[jobPrio].values(), key=itemgetter('timestamp')):
                jobid = job['id']
                jobType = job['type']
                possibleSites = job['possibleLocations']
                jobSubmitLogByPriority[jobPrio]['Total'] += 1
                # now look for sites with free pending slots
                for siteName in possibleSites:
                    if siteName not in self.currentRcThresholds:
                        logging.warn("Have a job for %s which is not in the resource control", siteName)
                        continue
                    
                    try:
                        totalPendingSlots = self.currentRcThresholds[siteName]["total_pending_slots"]
                        totalPendingJobs = self.currentRcThresholds[siteName]["total_pending_jobs"]
                        totalRunningSlots = self.currentRcThresholds[siteName]["total_running_slots"]
                        totalRunningJobs = self.currentRcThresholds[siteName]["total_running_jobs"]

                        taskPendingSlots = self.currentRcThresholds[siteName]['thresholds'][jobType]["pending_slots"]
                        taskPendingJobs = self.currentRcThresholds[siteName]['thresholds'][jobType]["task_pending_jobs"]
                        taskRunningSlots = self.currentRcThresholds[siteName]['thresholds'][jobType]["max_slots"]
                        taskRunningJobs = self.currentRcThresholds[siteName]['thresholds'][jobType]["task_running_jobs"]
                        taskPriority = self.currentRcThresholds[siteName]['thresholds'][jobType]["priority"]
                    except KeyError as ex:
                        msg = "Invalid key for site %s and job type %s\n" % (siteName, jobType)
                        msg += str(ex)
                        logging.error(msg)
                        continue

                    # check if site has free pending slots AND free pending task slots
                    if totalPendingJobs >= totalPendingSlots or taskPendingJobs >= taskPendingSlots:
                        jobSubmitLogBySites[siteName]["NoPendingSlot"] += 1
                        logging.debug("Found a job for %s which has no free pending slots", siteName)
                        continue
                    # check if site overall thresholds have free slots
                    if totalPendingJobs + totalRunningJobs >= totalPendingSlots + totalRunningSlots:
                        jobSubmitLogBySites[siteName]["NoRunningSlot"] += 1
                        logging.debug("Found a job for %s which has no free overall slots", siteName)
                        continue
                    # finally, check whether task has free overall slots
                    if taskPendingJobs + taskRunningJobs >= taskPendingSlots + taskRunningSlots:
                        jobSubmitLogBySites[siteName]["NoTaskSlot"] += 1
                        logging.debug("Found a job for %s which has no free task slots", siteName)
                        continue

                    # otherwise, update the site/task thresholds and the component job counter
                    self.currentRcThresholds[siteName]["total_pending_jobs"] += 1
                    self.currentRcThresholds[siteName]['thresholds'][jobType]["task_pending_jobs"] += 1
                    jobsCount += 1

                    # load (and remove) the job dictionary object from jobDataCache
                    cachedJob = self.jobDataCache.pop(jobid)
                    jobsToUncache.append((jobPrio, jobid))

                    # Sort jobs by jobPackage
                    package = cachedJob['packageDir']
                    if package not in jobsToSubmit.keys():
                        jobsToSubmit[package] = []

                    # Add the sandbox to a global list
                    self.sandboxPackage[package] = cachedJob.pop('sandbox')

                    # Now update the job dictionary object
                    cachedJob['custom'] = {'location': siteName}
                    cachedJob['taskPriority'] = taskPriority

                    # Get this job in place to be submitted by the plugin
                    jobsToSubmit[package].append(cachedJob)
                    
                    jobSubmitLogBySites[siteName]["submitted"] += 1
                    jobSubmitLogByPriority[jobPrio]['submitted'] += 1
                    # found a site to submit this job, so go to the next job
                    break

                # set the flag and get out of the job iteration
                if jobsCount >= self.maxJobsPerPoll:
                    exitLoop = True
                    break

        # jobs that are going to be submitted must be removed from all caches
        for prio, jobid in jobsToUncache:
            self.cachedJobs[prio].pop(jobid)
            self.cachedJobIDs.remove(jobid)
            
        logging.info("Site submission report: %s", dict(jobSubmitLogBySites))
        logging.info("Priority submission report: %s", dict(jobSubmitLogByPriority))
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

        for package in jobsToSubmit.keys():

            sandbox = self.sandboxPackage[package]
            jobs = jobsToSubmit.get(package, [])
            for job in jobs:
                job['location'], job['plugin'], job['site_cms_name'] = self.getSiteInfo(job['custom']['location'])
                job['sandbox'] = sandbox
                idList.append({'jobid': job['id'], 'location': job['custom']['location']})

            #Clean out the package reference
            del self.sandboxPackage[package]

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

        if not jobSite in self.locationDict.keys():
            siteInfo = self.locationAction.execute(siteName=jobSite)
            self.locationDict[jobSite] = siteInfo[0]
        return (self.locationDict[jobSite].get('ce_name'),
                self.locationDict[jobSite].get('plugin'),
                self.locationDict[jobSite].get('cms_name'))

    def algorithm(self, parameters=None):
        """
        _algorithm_

        Try to, in order:
        1) Refresh the cache
        2) Find jobs for all the necessary sites
        3) Submit the jobs to the plugin
        """

        try:
            myThread = threading.currentThread()
            self.getThresholds()
            self.refreshCache()
            
            if self.useReqMgrForCompletionCheck:
                # only runs when reqmgr is used (not Tier0)
                self.removeAbortedForceCompletedWorkflowFromCache()
            
            jobsToSubmit = self.assignJobLocations()
            self.submitJobs(jobsToSubmit=jobsToSubmit)
        except WMException:
            if getattr(myThread, 'transaction', None) != None:
                myThread.transaction.rollback()
            raise
        except Exception as ex:
            msg = 'Fatal error in JobSubmitter:\n'
            msg += str(ex)
            #msg += str(traceback.format_exc())
            msg += '\n\n'
            logging.error(msg)
            if getattr(myThread, 'transaction', None) != None:
                myThread.transaction.rollback()
            raise JobSubmitterPollerException(msg)

        return



    def terminate(self, params):
        """
        _terminate_

        Kill the code after one final pass when called by the master thread.
        """
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)
