#!/usr/bin/env python
#pylint: disable=W0102, W6501, C0301
# W0102: We want to pass blank lists by default
# for the whitelist and the blacklist
# W6501: pass information to logging using string arguments
# C0301: I'm ignoring this because breaking up error messages is painful
"""
_JobSubmitterPoller_t_

Submit jobs for execution.
"""

import random
import logging
import threading
import os.path
import cPickle

# WMBS objects
from WMCore.DAOFactory        import DAOFactory
from WMCore.WMExceptions      import WMJobErrorCodes

from WMCore.JobStateMachine.ChangeState       import ChangeState
from WMCore.WorkerThreads.BaseWorkerThread    import BaseWorkerThread
from WMCore.ResourceControl.ResourceControl   import ResourceControl
from WMCore.DataStructs.JobPackage            import JobPackage
from WMCore.FwkJobReport.Report               import Report
from WMCore.WMException                       import WMException
from WMCore.BossAir.BossAirAPI                import BossAirAPI

def siteListCompare(a, b):
    """
    _siteListCompare_

    Sites are stored as a tuple where the first element is the SE name and the
    second element is the number of free slots.  We'll sort based on the second
    element.
    """
    if a[1] > b[1]:
        return 1
    elif a[1] == b[1]:
        return 0

    return -1


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

        #DAO factory for WMBS objects
        self.daoFactory = DAOFactory(package = "WMCore.WMBS", \
                                     logger = logging,
                                     dbinterface = myThread.dbi)
        self.config = config

        #Libraries
        self.resourceControl = ResourceControl()

        self.changeState = ChangeState(self.config)
        self.repollCount = getattr(self.config.JobSubmitter, 'repollCount', 10000)
        self.maxJobsPerPoll = int(getattr(self.config.JobSubmitter, 'maxJobsPerPoll', 1000))

        # BossAir
        self.bossAir = BossAirAPI(config = self.config)

        # Additions for caching-based JobSubmitter
        self.workflowTimestamps = {}
        self.workflowPrios      = {}
        self.cachedJobIDs       = set()
        self.cachedJobs         = {}
        self.jobDataCache       = {}
        self.jobsToPackage      = {}
        self.sandboxPackage     = {}
        self.siteKeys           = {}
        self.locationDict       = {}
        self.cmsNames           = {}
        self.drainSites         = set()
        self.abortSites         = set()
        self.sortedSites        = []
        self.packageSize        = getattr(self.config.JobSubmitter, 'packageSize', 500)
        self.collSize           = getattr(self.config.JobSubmitter, 'collectionSize',
                                          self.packageSize * 1000)

        # initialize the alert framework (if available)
        self.initAlerts(compName = "JobSubmitter")

        try:
            if not getattr(self.config.JobSubmitter, 'submitDir', None):
                self.config.JobSubmitter.submitDir = self.config.JobSubmitter.componentDir
            self.packageDir = os.path.join(self.config.JobSubmitter.submitDir, 'packages')

            if not os.path.exists(self.packageDir):
                os.makedirs(self.packageDir)
        except Exception as ex:
            msg =  "Error while trying to create packageDir %s\n!"
            msg += str(ex)
            logging.error(msg)
            self.sendAlert(6, msg = msg)
            try:
                logging.debug("PackageDir: %s" % self.packageDir)
                logging.debug("Config: %s" % config)
            except:
                pass
            raise JobSubmitterPollerException(msg)


        # Now the DAOs
        self.listJobsAction = self.daoFactory(classname = "Jobs.ListForSubmitter")
        self.setLocationAction = self.daoFactory(classname = "Jobs.SetLocation")
        self.locationAction = self.daoFactory(classname = "Locations.GetSiteInfo")
        self.setFWJRPathAction = self.daoFactory(classname = "Jobs.SetFWJRPath")
        self.listWorkflows = self.daoFactory(classname = "Workflow.ListForSubmitter")

        # Keep a record of the thresholds in memory
        self.currentRcThresholds = {}

        return

    def getPackageCollection(self, sandboxDir):
        """
        _getPackageCollection_

        Given a jobID figure out which packageCollection
        it should belong in.
        """

        rawList     = os.listdir(sandboxDir)
        collections = []
        numberList  = []
        for entry in rawList:
            if 'PackageCollection' in entry:
                collections.append(entry)

        # If we have no collections, return 0 (PackageCollection_0)
        if len(collections) < 1:
            return 0

        # Loop over the list of PackageCollections
        for collection in collections:
            collectionPath = os.path.join(sandboxDir, collection)
            packageList    = os.listdir(collectionPath)
            collectionNum  = int(collection.split('_')[1])
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
            batchid    = "%s-%s" % (loadedJob["id"], loadedJob["retry_count"])
            sandboxDir = os.path.dirname(loadedJob["sandbox"])

            # Second, assemble the jobPackage location
            collectionIndex = self.getPackageCollection(sandboxDir)
            collectionDir   = os.path.join(sandboxDir,
                                           'PackageCollection_%i' % collectionIndex,
                                           'batch_%s' % batchid)

            # Now create the package object
            self.jobsToPackage[loadedJob["workflow"]] = {"batchid": batchid,
                                                         'id': loadedJob['id'],
                                                         "package": JobPackage(directory = collectionDir)}

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
            batchID    = self.jobsToPackage[workflowName]["batchid"]
            id         = self.jobsToPackage[workflowName]["id"]
            jobPackage = self.jobsToPackage[workflowName]["package"]
            batchDir   = jobPackage['directory']

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
        badJobs = dict([(x, []) for x in range(61101,61105)])
        dbJobs = set()

        logging.info("Refreshing priority cache...")
        workflows = self.listWorkflows.execute()
        workflows = filter(lambda x: x['name'] in self.workflowPrios, workflows)
        for workflow in workflows:
            self.workflowPrios[workflow['name']] = workflow['priority']

        logging.info("Querying WMBS for jobs to be submitted...")
        newJobs = self.listJobsAction.execute()
        logging.info("Found %s new jobs to be submitted." % len(newJobs))

        logging.info("Determining possible sites for new jobs...")
        jobCount = 0
        for newJob in newJobs:
            jobID = newJob['id']
            dbJobs.add(jobID)
            if jobID in self.cachedJobIDs:
                continue

            jobCount += 1
            if jobCount % 5000 == 0:
                logging.info("Processed %d/%d new jobs." % (jobCount, len(newJobs)))

            pickledJobPath = os.path.join(newJob["cache_dir"], "job.pkl")

            if not os.path.isfile(pickledJobPath):
                # Then we have a problem - there's no file
                logging.error("Could not find pickled jobObject %s" % pickledJobPath)
                badJobs[61103].append(newJob)
                continue
            try:
                jobHandle = open(pickledJobPath, "r")
                loadedJob = cPickle.load(jobHandle)
                jobHandle.close()
            except Exception as ex:
                msg =  "Error while loading pickled job object %s\n" % pickledJobPath
                msg += str(ex)
                logging.error(msg)
                self.sendAlert(6, msg = msg)
                raise JobSubmitterPollerException(msg)

            loadedJob['retry_count'] = newJob['retry_count']

            siteWhitelist = loadedJob.get("siteWhitelist", [])
            siteBlacklist = loadedJob.get("siteBlacklist", [])
            trustSitelists = loadedJob.get("trustSitelists", False)

            # convert site lists into correct format
            if len(siteWhitelist) > 0:
                whitelist = []
                for cmsName in siteWhitelist:
                    whitelist.extend(self.cmsNames.get(cmsName, []))
                siteWhitelist = whitelist
            if len(siteBlacklist) > 0:
                blacklist = []
                for cmsName in siteBlacklist:
                    blacklist.extend(self.cmsNames.get(cmsName, []))
                siteBlacklist = blacklist

            # figure out possible locations for job
            if trustSitelists:
                possibleLocations = set(siteWhitelist) - set(siteBlacklist)
            else:
                possibleLocations = set()

                # all files in job have same location (in se names)
                #rawLocations = loadedJob["input_files"][0]["locations"]
                rawLocations = loadedJob.get("inputDatasetLocations", [])

                # transform se names into site names
                for loc in rawLocations:
                    if not loc in self.siteKeys.keys():
                        # Then we have a problem
                        logging.error('Encountered unknown location %s for job %i' % (loc, jobID))
                        logging.error('Ignoring for now, but watch out for this')
                    else:
                        for siteName in self.siteKeys[loc]:
                            possibleLocations.add(siteName)

                # filter with site lists
                if len(siteWhitelist) > 0:
                    possibleLocations = possibleLocations & set(siteWhitelist)
                if len(siteBlacklist) > 0:
                    possibleLocations = possibleLocations - set(siteBlacklist)

            # Create another set of locations that may change when a site goes white/black listed
            # Does not care about the non_draining or aborted sites, they may change and that is the point
            potentialLocations = set()
            potentialLocations.update(possibleLocations)

            # now check for sites in drain and adjust the possible locations
            # also check if there is at least one site left to run the job
            if len(possibleLocations) == 0:
                newJob['name'] = loadedJob['name']
                badJobs[61101].append(newJob)
                continue
            else :
                non_abort_sites = [x for x in possibleLocations if x not in self.abortSites]
                if non_abort_sites: # if there is at least a non aborted/down site then run there, otherwise fail the job
                    possibleLocations = non_abort_sites
                else:
                    newJob['name'] = loadedJob['name']
                    newJob['possibleLocations'] = possibleLocations
                    badJobs[61102].append(newJob)
                    continue

            # try to remove draining sites if possible, this is needed to stop
            # jobs that could run anywhere blocking draining sites
            # if the job type is Merge, LogCollect or Cleanup this is skipped
            if newJob['type'] not in ('LogCollect','Merge','Cleanup','Harvesting'):
                non_draining_sites = [x for x in possibleLocations if x not in self.drainSites]
                if non_draining_sites: # if >1 viable non-draining site remove draining ones
                    possibleLocations = non_draining_sites
                else:
                    newJob['name'] = loadedJob['name']
                    newJob['possibleLocations'] = possibleLocations
                    badJobs[61104].append(newJob)
                    continue

            batchDir = self.addJobsToPackage(loadedJob)
            self.cachedJobIDs.add(jobID)

            for possibleLocation in possibleLocations:
                if possibleLocation not in self.cachedJobs:
                    self.cachedJobs[possibleLocation] = {}
                if newJob["type"] not in self.cachedJobs[possibleLocation]:
                    self.cachedJobs[possibleLocation][newJob["type"]] = {}

                locTypeCache = self.cachedJobs[possibleLocation][newJob["type"]]
                workflowName = newJob['workflow']
                timestamp    = newJob['timestamp']
                prio         = newJob['task_priority']
                if workflowName not in locTypeCache:
                    locTypeCache[workflowName] = set()
                if workflowName not in self.jobDataCache:
                    self.jobDataCache[workflowName] = {}
                if not workflowName in self.workflowTimestamps:
                    self.workflowTimestamps[workflowName] = timestamp
                if workflowName not in self.workflowPrios:
                    self.workflowPrios[workflowName] = prio

                locTypeCache[workflowName].add(jobID)

            # allow job baggage to override numberOfCores
            #       => used for repacking to get more slots/disk
            numberOfCores = loadedJob.get('numberOfCores', 1)
            if numberOfCores == 1:
                baggage = loadedJob.getBaggage()
                numberOfCores = getattr(baggage, "numberOfCores", 1)
            loadedJob['numberOfCores'] = numberOfCores

            # Now that we're out of that loop, put the job data in the cache
            jobInfo = (jobID,
                       newJob["retry_count"],
                       batchDir,
                       loadedJob["sandbox"],
                       loadedJob["cache_dir"],
                       loadedJob.get("ownerDN", None),
                       loadedJob.get("ownerGroup", ''),
                       loadedJob.get("ownerRole", ''),
                       frozenset(possibleLocations),
                       loadedJob.get("scramArch", None),
                       loadedJob.get("swVersion", None),
                       loadedJob["name"],
                       loadedJob.get("proxyPath", None),
                       newJob['request_name'],
                       loadedJob.get("estimatedJobTime", None),
                       loadedJob.get("estimatedDiskUsage", None),
                       loadedJob.get("estimatedMemoryUsage", None),
                       newJob['task_name'],
                       frozenset(potentialLocations),
                       loadedJob.get("numberOfCores", 1),
                       newJob['task_id'],
                       loadedJob.get('inputDataset', None),
                       loadedJob.get('inputDatasetLocations', None)
                       )

            self.jobDataCache[workflowName][jobID] = jobInfo

        # Register failures in submission
        for errorCode in badJobs:
            if badJobs[errorCode]:
                logging.debug("The following jobs could not be submitted: %s, error code : %d" % (badJobs, errorCode))
                self._handleSubmitFailedJobs(badJobs[errorCode], errorCode)

        # If there are any leftover jobs, we want to get rid of them.
        self.flushJobPackages()
        logging.info("Done with refreshCache() loop, pruning killed jobs.")

        # We need to remove any jobs from the cache that were not returned in
        # the last call to the database.
        jobIDsToPurge = self.cachedJobIDs - dbJobs
        self.cachedJobIDs -= jobIDsToPurge

        if len(jobIDsToPurge) == 0:
            return

        for siteName in self.cachedJobs.keys():
            for taskType in self.cachedJobs[siteName].keys():
                for workflow in self.cachedJobs[siteName][taskType].keys():
                    for cachedJobID in list(self.cachedJobs[siteName][taskType][workflow]):
                        if cachedJobID in jobIDsToPurge:
                            self.cachedJobs[siteName][taskType][workflow].remove(cachedJobID)
                            try:
                                del self.jobDataCache[workflow][cachedJobID]
                            except KeyError:
                                # Already gone
                                pass

        logging.info("Done pruning killed jobs, moving on to submit.")
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
            if exitCode in (61102, 61104):
                job['fwjr'].addError("JobSubmit", exitCode, "SubmitFailed", WMJobErrorCodes[exitCode] + ', '.join(job['possibleLocations']))
            else:
                job['fwjr'].addError("JobSubmit", exitCode, "SubmitFailed", WMJobErrorCodes[exitCode] )
            fwjrPath = os.path.join(job['cache_dir'],
                                    'Report.%d.pkl' % int(job['retry_count']))
            job['fwjr'].setJobID(job['id'])
            try:
                job['fwjr'].save(fwjrPath)
                fwjrBinds.append({"jobid" : job["id"], "fwjrpath" : fwjrPath})
            except IOError as ioer:
                logging.error("Failed to write FWJR for submit failed job %d, message: %s" % (job['id'], str(ioer)))
        self.changeState.propagate(badJobs, "submitfailed", "created")
        self.setFWJRPathAction.execute(binds = fwjrBinds)
        return

    def getThresholds(self):
        """
        _getThresholds_

        Reformat the submit thresholds.  This will store a dictionary keyed by
        task type.  Each task type will contain a list of tuples where each
        tuple contains teh site name and the number of running jobs.
        """
        rcThresholds = self.resourceControl.listThresholdsForSubmit()

        newDrainSites = set()
        newAbortSites = set()

        for siteName in rcThresholds.keys():
            # Add threshold if we don't have it already
            cmsName = rcThresholds[siteName]["cms_name"]
            state   = rcThresholds[siteName]["state"]

            if not cmsName in self.cmsNames.keys():
                self.cmsNames[cmsName] = []
            if not siteName in self.cmsNames[cmsName]:
                self.cmsNames[cmsName].append(siteName)
            if state == "Draining":
                newDrainSites.add(siteName)
            if state in ["Down", "Aborted"]:
                newAbortSites.add(siteName)

            for seName in rcThresholds[siteName]["se_names"]:
                if not seName in self.siteKeys.keys():
                    self.siteKeys[seName] = []
                if not siteName in self.siteKeys[seName]:
                    self.siteKeys[seName].append(siteName)

        # When the list of drain/abort sites changes between iteration then a location
        # refresh is needed, for now it forces a full cache refresh
        # TODO: Make it more efficient, only reshuffle locations when this happens
        if newDrainSites != self.drainSites or  newAbortSites != self.abortSites:
            logging.info("Draining or Aborted sites have changed, the cache will be rebuilt.")
            self.cachedJobIDs       = set()
            self.cachedJobs         = {}
            self.jobDataCache       = {}

        #Sort the sites using the following criteria:
        #T1 sites go first, then T2, then T3
        #After that we fill first the bigger ones
        #Python sorting is stable so let's do 2 sort passes, it should be fast
        #Assume  that all CMS names start with T[0-9]+, which the lexicon guarantees
        self.sortedSites = sorted(rcThresholds.keys(),
                                  key = lambda x : rcThresholds[x]["total_pending_slots"],
                                  reverse = True)
        self.sortedSites = sorted(self.sortedSites, key = lambda x : rcThresholds[x]["cms_name"][0:2])
        logging.debug('Sites will be filled in the following order: %s' % str(self.sortedSites))

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
        jobsToPrune = {}
        jobsCount = 0
        exitLoop = False 

        for siteName in self.sortedSites:
            if exitLoop:
                break

            totalPending = None
            if siteName not in self.cachedJobs:
                logging.debug("No jobs for site %s" % siteName)
                continue
            logging.debug("Have site %s" % siteName)
            try:
                totalPendingSlots   = self.currentRcThresholds[siteName]["total_pending_slots"]
                totalRunningSlots   = self.currentRcThresholds[siteName]["total_running_slots"]
                totalRunning        = self.currentRcThresholds[siteName]["total_running_jobs"]
                totalPending        = self.currentRcThresholds[siteName]["total_pending_jobs"]
                state               = self.currentRcThresholds[siteName]["state"]
            except KeyError as ex:
                msg =  "Had invalid site info %s\n" % siteName['thresholds']
                msg += str(ex)
                logging.error(msg)
                continue
            for threshold in self.currentRcThresholds[siteName].get('thresholds', []):
                if exitLoop:
                    break
                try:
                    # Pull basic info for the threshold
                    taskType            = threshold["task_type"]
                    maxSlots            = threshold["max_slots"]
                    taskPendingSlots    = threshold["pending_slots"]
                    taskRunning         = threshold["task_running_jobs"]
                    taskPending         = threshold["task_pending_jobs"]
                    taskPriority        = threshold["priority"]
                except KeyError as ex:
                    msg =  "Had invalid threshold %s\n" % threshold
                    msg += str(ex)
                    logging.error(msg)
                    continue

                #If the site is down, do not submit anything to it
                if state == 'Down':
                    continue

                #If the number of running exceeded the running slots in the
                #site then get out
                if totalRunningSlots >= 0 and totalRunning >= totalRunningSlots:
                    continue

                #If the task is running more than allowed then get out
                if maxSlots >= 0 and taskRunning >= maxSlots:
                    continue

                # Ignore this threshold if we've cleaned out the site
                if siteName not in self.cachedJobs:
                    continue

                # Ignore this threshold if we have no jobs
                # for it
                if taskType not in self.cachedJobs[siteName]:
                    continue

                taskCache = self.cachedJobs[siteName][taskType]

                # Calculate number of jobs we need
                nJobsRequired = min(totalPendingSlots - totalPending, taskPendingSlots - taskPending)
                breakLoop = False
                logging.debug("nJobsRequired for task %s: %i" % (taskType, nJobsRequired))

                while nJobsRequired > 0:
                    # Do this until we have all the jobs for this threshold

                    # Pull a job out of the cache for the task/site.  Verify that we
                    # haven't already used this job in this polling cycle.
                    cachedJob = None
                    cachedJobWorkflow = None

                    workflows = taskCache.keys()
                    # Sorting by prio and timestamp on the subscription
                    def sortingCmp(x, y):
                        if (x not in self.workflowTimestamps) or (y not in self.workflowTimestamps):
                            return -1
                        if (x not in self.workflowPrios) or (y not in self.workflowPrios):
                            return -1
                        if self.workflowPrios[x] > self.workflowPrios[y]:
                            return -1
                        elif self.workflowPrios[x] == self.workflowPrios[y]:
                            return cmp(self.workflowTimestamps[x], self.workflowTimestamps[y])
                        else:
                            return 1
                    workflows.sort(cmp = sortingCmp)

                    for workflow in workflows:
                        # Run a while loop until you get a job
                        while len(taskCache[workflow]) > 0:
                            cachedJobID = taskCache[workflow].pop()
                            try:
                                cachedJob   = self.jobDataCache[workflow].get(cachedJobID, None)
                                del self.jobDataCache[workflow][cachedJobID]
                            except:
                                pass

                            if cachedJobID not in jobsToPrune.get(workflow, set()):
                                cachedJobWorkflow = workflow
                                break
                            else:
                                cachedJob = None

                        # Remove the entry in the cache for the workflow if it is empty.
                        if len(self.cachedJobs[siteName][taskType][workflow]) == 0:
                            del self.cachedJobs[siteName][taskType][workflow]
                        if workflow in self.jobDataCache and len(self.jobDataCache[workflow].keys()) == 0:
                            del self.jobDataCache[workflow]

                        if cachedJob:
                            # We found a job, bail out and handle it.
                            break

                    # Check to see if we need to delete this site from the cache
                    if len(self.cachedJobs[siteName][taskType].keys()) == 0:
                        del self.cachedJobs[siteName][taskType]
                        breakLoop = True
                    if len(self.cachedJobs[siteName].keys()) == 0:
                        del self.cachedJobs[siteName]
                        breakLoop = True

                    if not cachedJob:
                        # We didn't find a job, bail out.
                        # This site and task type is done
                        break

                    self.cachedJobIDs.remove(cachedJob[0])

                    if cachedJobWorkflow not in jobsToPrune:
                        jobsToPrune[cachedJobWorkflow] = set()

                    jobsToPrune[cachedJobWorkflow].add(cachedJob[0])

                    # Sort jobs by jobPackage
                    package = cachedJob[2]
                    if not package in jobsToSubmit.keys():
                        jobsToSubmit[package] = []

                    # Add the sandbox to a global list
                    self.sandboxPackage[package] = cachedJob[3]

                    possibleSites = cachedJob[8]
                    possibleSiteList = list(possibleSites)
                    fakeAssignedSiteName = random.choice(possibleSiteList)
                    potentialSites = cachedJob[18]

                    # Create a job dictionary object
                    jobDict = {'id': cachedJob[0],
                               'retry_count': cachedJob[1],
                               'custom': {'location': fakeAssignedSiteName},
                               'cache_dir': cachedJob[4],
                               'packageDir': package,
                               'userdn': cachedJob[5],
                               'usergroup': cachedJob[6],
                               'userrole': cachedJob[7],
                               'priority': taskPriority,
                               'taskType': taskType,
                               'possibleSites': possibleSites,
                               'scramArch': cachedJob[9],
                               'swVersion': cachedJob[10],
                               'name': cachedJob[11],
                               'proxyPath': cachedJob[12],
                               'requestName': cachedJob[13],
                               'estimatedJobTime' : cachedJob[14],
                               'estimatedDiskUsage' : cachedJob[15],
                               'estimatedMemoryUsage' : cachedJob[16],
                               'taskPriority' : self.workflowPrios[workflow],
                               'taskName' : cachedJob[17],
                               'potentialSites' : potentialSites,
                               'numberOfCores' : cachedJob[19],
                               'taskID' : cachedJob[20],
                               'inputDataset' : cachedJob[21],
                               'inputDatasetLocations' : cachedJob[22]
                               }

                    # Add to jobsToSubmit
                    jobsToSubmit[package].append(jobDict)
                    jobsCount += 1
                    if jobsCount >= self.maxJobsPerPoll:
                        breakLoop, exitLoop = True, True

                    # Deal with accounting
                    if len(possibleSites) == 1:
                        nJobsRequired -= 1
                    totalPending  += 1
                    taskPending   += 1

                    if breakLoop:
                        break

        # Remove the jobs that we're going to submit from the cache.
        allWorkflows = set()
        for siteName in self.cachedJobs.keys():
            for taskType in self.cachedJobs[siteName].keys():
                for workflow in self.cachedJobs[siteName][taskType].keys():
                    allWorkflows.add(workflow)
                    if workflow in jobsToPrune.keys():
                        self.cachedJobs[siteName][taskType][workflow] -= jobsToPrune[workflow]

        # Remove workflows from the timestamp dictionary which are not anymore in the cache
        workflowsWithTimestamp = self.workflowTimestamps.keys()
        for workflow in workflowsWithTimestamp:
            if workflow not in allWorkflows:
                del self.workflowTimestamps[workflow]

        workflowsWithPrios = self.workflowPrios.keys()
        for workflow in workflowsWithPrios:
            if workflow not in allWorkflows:
                del self.workflowPrios[workflow]

        logging.info("Have %s packages to submit." % len(jobsToSubmit))
        logging.info("Done assigning site locations.")
        return jobsToSubmit


    def submitJobs(self, jobsToSubmit):
        """
        _submitJobs_

        Actually do the submission of the jobs
        """

        jobList   = []
        idList    = []

        if len(jobsToSubmit) == 0:
            logging.debug("There are no packages to submit.")
            return

        for package in jobsToSubmit.keys():

            sandbox = self.sandboxPackage[package]
            jobs    = jobsToSubmit.get(package, [])

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
        successList, failList = self.bossAir.submit(jobs = jobList)
        logging.info("Jobs that succeeded/failed submission: %d/%d." % (len(successList),len(failList)))

        # Propagate states in the WMBS database
        logging.debug("Propagating success state to WMBS.")
        self.changeState.propagate(successList, 'executing', 'created')
        logging.debug("Propagating fail state to WMBS.")
        self.changeState.propagate(failList, 'submitfailed', 'created')

        # At the end we mark the locations of the jobs
        # This applies even to failed jobs, since the location
        # could be part of the failure reason.
        logging.debug("Updating job location...")
        self.setLocationAction.execute(bulkList = idList, conn = myThread.transaction.conn,
                                       transaction = True)
        myThread.transaction.commit()
        logging.info("Transaction cycle successfully completed.")

        return


    def getSiteInfo(self, jobSite):
        """
        _getSiteInfo_

        This is how you get the name of a CE and the plugin for a job
        """

        if not jobSite in self.locationDict.keys():
            siteInfo = self.locationAction.execute(siteName = jobSite)
            self.locationDict[jobSite] = siteInfo[0]
        return (self.locationDict[jobSite].get('ce_name'),
                self.locationDict[jobSite].get('plugin'),
                self.locationDict[jobSite].get('cms_name'))

    def algorithm(self, parameters = None):
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
            jobsToSubmit = self.assignJobLocations()
            self.submitJobs(jobsToSubmit = jobsToSubmit)


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
            self.sendAlert(7, msg = msg)
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
