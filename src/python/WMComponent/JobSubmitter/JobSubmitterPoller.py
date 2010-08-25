#!/usr/bin/env python
#pylint: disable-msg=W0102, W6501, C0301
# W0102: We want to pass blank lists by default
# for the whitelist and the blacklist
# W6501: pass information to logging using string arguments
# C0301: I'm ignoring this because breaking up error messages is painful

"""
Creates jobs for new subscriptions

"""





#This job currently depends on the following config variables in JobSubmitter:
# pluginName
# pluginDir

import logging
import threading
import os.path
import cPickle
import traceback

# WMBS objects
from WMCore.DAOFactory        import DAOFactory

from WMCore.JobStateMachine.ChangeState       import ChangeState
from WMCore.WorkerThreads.BaseWorkerThread    import BaseWorkerThread
from WMCore.ProcessPool.ProcessPool           import ProcessPool
from WMCore.ResourceControl.ResourceControl   import ResourceControl
from WMCore.DataStructs.JobPackage            import JobPackage
from WMCore.WMBase        import getWMBASE



#pylint: disable-msg=C0103
def _VmB(VmKey):
    '''Private.

    Code taken from:
    http://code.activestate.com/recipes/286222-memory-usage/
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

        configDict = {"submitDir": self.config.JobSubmitter.submitDir,
                      "submitNode": self.config.JobSubmitter.submitNode,
                      "agentName": self.config.Agent.agentName,
                      'couchURL': self.config.JobStateMachine.couchurl, 
                      'defaultRetries': self.config.JobStateMachine.default_retries,
                      'couchDBName': self.config.JobStateMachine.couchDBName}
        
        if hasattr(self.config.JobSubmitter, "submitScript"):
            configDict["submitScript"] = self.config.JobSubmitter.submitScript
        else:
            configDict["submitScript"] = os.path.join(getWMBASE(),
                                                      "src/python/WMComponent/JobSubmitter/submit.sh")

        if hasattr(self.config.JobSubmitter, 'inputFile'):
            configDict['inputFile'] = self.config.JobSubmitter.inputFile


        workerName = "%s.%s" % (self.config.JobSubmitter.pluginDir, \
                                self.config.JobSubmitter.pluginName)

        self.processPool = ProcessPool(workerName,
                                       totalSlaves = self.config.JobSubmitter.workerThreads,
                                       componentDir = self.config.JobSubmitter.componentDir,
                                       config = self.config, slaveInit = configDict,
                                       namespace = getattr(self.config.JobSubmitter, "pluginNamespace", None))


        self.changeState = ChangeState(self.config)
        self.repollCount = getattr(self.config.JobSubmitter, 'repollCount', 10000)

        # Additions for caching-based JobSubmitter
        self.cachedJobIDs   = set()
        self.cachedJobs     = {}
        self.jobsToPackage  = {}
        self.sandboxPackage = {}
        self.siteKeys       = {}
        self.packageSize    = getattr(self.config.JobSubmitter, 'packageSize', 100)

        self.packageDir = os.path.join(self.config.JobSubmitter.submitDir, "packages")
        if not os.path.exists(self.packageDir):
            os.makedirs(self.packageDir)

        self.listJobsAction = self.daoFactory(classname = "Jobs.ListForSubmitter")
        self.setLocationAction = self.daoFactory(classname = "Jobs.SetLocation")

        # Call once to fill the siteKeys
        # TODO: Make this less clumsy!
        self.getThresholds()
        return

    def addJobsToPackage(self, loadedJob):
        """
        _addJobsToPackage_

        Add a job to a job package and then return the batch ID for the job.
        Packages are only written out to disk when they contain 100 jobs.  The
        flushJobsPackages() method must be called after all jobs have been added
        to the cache and before they are actually submitted to make sure all the
        job packages have been written to disk.
        """
        if not self.jobsToPackage.has_key(loadedJob["workflow"]):
            batchid = "%s-%s" % (loadedJob["id"], loadedJob["retry_count"])
            self.jobsToPackage[loadedJob["workflow"]] = {"batchid": batchid,
                                                         "package": JobPackage()}

        jobPackage = self.jobsToPackage[loadedJob["workflow"]]["package"]
        jobPackage[loadedJob["id"]] = loadedJob.getDataStructsJob()

        batchID = self.jobsToPackage[loadedJob["workflow"]]["batchid"]
        sandboxDir = os.path.dirname(jobPackage[jobPackage.keys()[0]]["sandbox"])
        batchDir = os.path.join(sandboxDir, "batch_%s" % batchID)
        
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
            batchID = self.jobsToPackage[workflowName]["batchid"]
            jobPackage = self.jobsToPackage[workflowName]["package"]

            sandboxDir = os.path.dirname(jobPackage[jobPackage.keys()[0]]["sandbox"])
            batchDir = os.path.join(sandboxDir, "batch_%s" % batchID)

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
        don't unpickle them and combine their site white and black list with
        the list of locations they can run at.  Add them to the cache.

        Each entry in the cache is a tuple with five items:
          - WMBS Job ID
          - Retry count
          - Batch ID
          - Path to sanbox
          - Path to cache directory
        """
        badJobs = []

        logging.info("Querying WMBS for jobs to be submitted...")
        newJobs = self.listJobsAction.execute()
        logging.info("Found %s new jobs to be submitted." % len(newJobs))

        logging.info("Determining possible sites for new jobs...")
        jobCount = 0
        for newJob in newJobs:
            jobCount += 1
            jobID = newJob['id']
            if jobID in self.cachedJobIDs:
                continue

            if jobCount % 5000 == 0:
                logging.info("Processed %d/%d new jobs." % (jobCount, len(newJobs)))

            pickledJobPath = os.path.join(newJob["cache_dir"], "job.pkl")
            jobHandle = open(pickledJobPath, "r")
            loadedJob = cPickle.load(jobHandle)

            loadedJob['retry_count'] = newJob['retry_count']

            # Grab the possible locations
            # This should be in terms of siteNames
            # Because there can be multiple entry points to a site with one SE
            # And each of them can be a separate location
            # Note that all the files in a job have the same set of locations
            possibleLocations = set()
            rawLocations      = loadedJob["input_files"][0]["locations"]

            # Transform se into siteNames
            for loc in rawLocations:
                if not loc in self.siteKeys.keys():
                    # Then we have a problem
                    logging.error('Encountered unknown location %s for job %i' % (loc, jobID))
                    logging.error('Ignoring for now, but watch out for this')
                else:
                    for siteName in self.siteKeys[loc]:
                        possibleLocations.add(siteName)
            
            if len(loadedJob["siteWhitelist"]) > 0:
                possibleLocations = possibleLocations & set(loadedJob.get("siteWhitelist"))
            if len(loadedJob["siteBlacklist"]) > 0:
                possibleLocations = possibleLocations - set(loadedJob.get("siteBlacklist"))

            if len(possibleLocations) == 0:
                badJobs.append(newJob)
                continue

            batchDir = self.addJobsToPackage(loadedJob)
            self.cachedJobIDs.add(jobID)

            if not self.cachedJobs.has_key(newJob["workflow"]):
                self.cachedJobs[newJob["workflow"]] = {}

            workflowCache = self.cachedJobs[newJob["workflow"]]

            for possibleLocation in possibleLocations:
                if not self.cachedJobs.has_key(possibleLocation):
                    self.cachedJobs[possibleLocation] = {}
                if not self.cachedJobs[possibleLocation].has_key(newJob["type"]):
                    self.cachedJobs[possibleLocation][newJob["type"]] = {}

                locTypeCache = self.cachedJobs[possibleLocation][newJob["type"]]
                if not locTypeCache.has_key(newJob["workflow"]):
                    locTypeCache[newJob["workflow"]] = set()
                
                locTypeCache[newJob["workflow"]].add((jobID,
                                                      newJob["retry_count"],
                                                      batchDir,
                                                      loadedJob["sandbox"],
                                                      loadedJob["cache_dir"]))
                
        if len(badJobs) > 0:
            logging.error("The following jobs have no possible sites to run at: %s" % badJobs)
            for job in badJobs:
                job['couch_record'] = None
            self.changeState.propagate(badJobs, "submitfailed", "created")

        # If there are any leftover jobs, we want to get rid of them.
        self.flushJobPackages()
        logging.info("Done with refreshCache() loop, submitting jobs.")
        return

    def getThresholds(self):
        """
        _getThresholds_

        Reformat the submit thresholds.  This will return a dictionary keyed by
        task type.  Each task type will contain a list of tuples where each
        tuple contains teh site name and the number of running jobs.
        """
        rcThresholds = self.resourceControl.listThresholdsForSubmit()

        submitThresholds = {}
        for siteName in rcThresholds.keys():
            for taskType in rcThresholds[siteName].keys():
                seName = rcThresholds[siteName][taskType]["se_name"]
                if not seName in self.siteKeys.keys():
                    self.siteKeys[seName] = []
                self.siteKeys[seName].append(siteName)

                if not submitThresholds.has_key(taskType):
                    submitThresholds[taskType] = []

                maxSlots = rcThresholds[siteName][taskType]["max_slots"]
                runningJobs = rcThresholds[siteName][taskType]["task_running_jobs"]                

                if runningJobs < maxSlots:
                    submitThresholds[taskType].append((siteName,
                                                       maxSlots - runningJobs))

        return submitThresholds

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
        submitThresholds = self.getThresholds()

        jobsToSubmit = {}
        jobsToPrune = {}

        for taskType in submitThresholds.keys():
            logging.info("Assigning locations for task %s" % taskType)

            siteList = submitThresholds[taskType]
            while len(siteList) > 0:
                # Sort the list of sites that have open slots for this task type
                # and task the one that has the most free slots.
                siteList.sort(siteListCompare)
                emptySite = siteList.pop(0)

                # If we don't have any jobs for this site then move on.
                if not self.cachedJobs.has_key(emptySite[0]):
                    continue
                if not self.cachedJobs[emptySite[0]].has_key(taskType):
                    continue

                # Pull a job out of the cache for the task/site.  Verify that we
                # haven't already used this job in this polling cycle.
                cachedJob = None
                cachedJobWorkflow = None

                workflows = self.cachedJobs[emptySite[0]][taskType].keys()
                workflows.sort()

                for workflow in workflows:
                    while len(self.cachedJobs[emptySite[0]][taskType][workflow]) > 0:
                        cachedJob = self.cachedJobs[emptySite[0]][taskType][workflow].pop()

                        if cachedJob not in jobsToPrune.get(workflow, set()):
                            cachedJobWorkflow = workflow
                            break
                        else:
                            cachedJob = None

                    # Remove the entry in the cache for the workflow if it is empty.
                    if len(self.cachedJobs[emptySite[0]][taskType][workflow]) == 0:
                        del self.cachedJobs[emptySite[0]][taskType][workflow]

                    if cachedJob:
                        # We found a job, bail out and handle it.
                        break

                if len(self.cachedJobs[emptySite[0]][taskType].keys()) == 0:
                    del self.cachedJobs[emptySite[0]][taskType]
                if len(self.cachedJobs[emptySite[0]].keys()) == 0:
                    del self.cachedJobs[emptySite[0]]

                if not cachedJob:
                    # We didn't find a job, bail out.
                    continue
                
                self.cachedJobIDs.remove(cachedJob[0])

                if not jobsToPrune.has_key(cachedJobWorkflow):
                    jobsToPrune[cachedJobWorkflow] = set()
                    
                jobsToPrune[cachedJobWorkflow].add(cachedJob)
                        
                # Sort jobs by jobPackage
                package = cachedJob[2]
                if not package in jobsToSubmit.keys():
                    jobsToSubmit[package] = []

                # Add the sandbox to a global list
                self.sandboxPackage[package] = cachedJob[3]

                # Create a job dictionary object
                jobDict = {'id': cachedJob[0],
                           'retry_count': cachedJob[1],
                           'custom': {'location': emptySite[0]},
                           'cache_dir': cachedJob[4]}

                # Add to jobsToSubmit
                jobsToSubmit[package].append(jobDict)

                # If we were able to pull down a job for the task/site and the
                # site it not full we'll still add more jobs to it.
                if emptySite[1] - 1 > 0:
                    siteList.append((emptySite[0], emptySite[1] - 1))

        # Remove the jobs that we're going to submit from the cache.
        for siteName in self.cachedJobs.keys():
            for taskType in self.cachedJobs[siteName].keys():
                for workflow in self.cachedJobs[siteName][taskType].keys():
                    if workflow in jobsToPrune.keys():
                        self.cachedJobs[siteName][taskType][workflow] -= jobsToPrune[workflow]

        logging.info("Have %s jobs to submit." % len(jobsToSubmit))
        logging.info("Done assigning site locations.")
        return jobsToSubmit

    def submitJobs(self, jobsToSubmit):
        """
        _submitJobs_

        Actually do the submission of the jobs
        """

        agentName = self.config.Agent.agentName
        lenWork   = 0

        for package in jobsToSubmit.keys():
            sandbox = self.sandboxPackage[package]
            jobs    = jobsToSubmit.get(package, [])

            #Clean out the package reference
            del self.sandboxPackage[package]
            
            if len(jobs) == 0:
                # No jobs in this package
                continue
            while len(jobs) > 0:
                # Then we have to split a chunk off and submit it
                jobsReady = jobs[:self.config.JobSubmitter.jobsPerWorker]
                jobs      = jobs[self.config.JobSubmitter.jobsPerWorker:]
                self.processPool.enqueue([{'jobs': jobsReady,
                                           'packageDir': package,
                                           'sandbox': sandbox,
                                           'agentName': agentName}])
                lenWork += 1



        # And then, at the end of the day, we have to dequeue them.
        result = []
        result = self.processPool.dequeue(lenWork)
       

        return

    def algorithm(self, parameters = None):
        """
        _algorithm_

        Try to, in order:
        1) Refresh the cache
        2) Find jobs for all the necessary sites
        3) Submit the jobs to the plugin
        """
        try:
            self.refreshCache()
            jobsToSubmit = self.assignJobLocations()
            self.submitJobs(jobsToSubmit = jobsToSubmit)
        except Exception, ex:
            msg = 'Fatal error in JobSubmitter:\n'
            msg += str(ex)
            msg += str(traceback.format_exc())
            msg += '\n\n'
            logging.error(msg)
            raise Exception(msg)

        # At the end we mark the locations of the jobs
        # This applies even to failed jobs, since the location
        # could be part of the failure reason.
        idList = []
        for package in jobsToSubmit.keys():
            for job in jobsToSubmit.get(package, []):
                idList.append({'jobid': job['id'], 'location': job['custom']['location']})
        self.setLocationAction.execute(bulkList = idList)



        #logging.error("About to print memory sizes")
        #logging.error(_VmB('VmSize:'))
        #logging.error(_VmB('VmStk:'))

        return

