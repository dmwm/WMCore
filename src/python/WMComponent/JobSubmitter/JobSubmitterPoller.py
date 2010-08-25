#!/usr/bin/env python
#pylint: disable-msg=W0102, W6501, C0301
# W0102: We want to pass blank lists by default
# for the whitelist and the blacklist
# W6501: pass information to logging using string arguments
# C0301: I'm ignoring this because breaking up error messages is painful

"""
Creates jobs for new subscriptions

"""

__revision__ = "$Id: JobSubmitterPoller.py,v 1.38 2010/07/29 14:21:27 sfoulkes Exp $"
__version__ = "$Revision: 1.38 $"


#This job currently depends on the following config variables in JobSubmitter:
# pluginName
# pluginDir

import logging
import threading
import os.path
import cPickle
import traceback

# WMBS objects
from WMCore.WMBS.Job          import Job
from WMCore.DAOFactory        import DAOFactory

from WMCore.JobStateMachine.ChangeState       import ChangeState
from WMCore.WorkerThreads.BaseWorkerThread    import BaseWorkerThread
from WMCore.ProcessPool.ProcessPool           import ProcessPool
from WMCore.ResourceControl.ResourceControl   import ResourceControl
from WMCore.DataStructs.JobPackage            import JobPackage
from WMCore.WMBase        import getWMBASE

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
    def __init__(self, config):

        myThread = threading.currentThread()

        #DAO factory for WMBS objects
        self.daoFactory = DAOFactory(package = "WMCore.WMBS", \
                                     logger = logging,
                                     dbinterface = myThread.dbi)

        #Dictionary definitions
        self.slots     = {}
        self.sites     = {}
        self.locations = {}

        self.session = None
        self.schedulerConfig = {}
        self.config = config
        self.types = []

        #Libraries
        self.resourceControl = ResourceControl()

        BaseWorkerThread.__init__(self)

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

        if hasattr(self.config, 'BossAir'):
            configDict['pluginName'] = config.BossAir.pluginName
            configDict['pluginName'] = config.BossAir.pluginDir 

        workerName = "%s.%s" % (self.config.JobSubmitter.pluginDir, \
                                self.config.JobSubmitter.pluginName)

        self.processPool = ProcessPool(workerName,
                                       totalSlaves = self.config.JobSubmitter.workerThreads,
                                       componentDir = self.config.JobSubmitter.componentDir,
                                       config = self.config, slaveInit = configDict,
                                       namespace = getattr(self.config.JobSubmitter, "pluginNamespace", None))


        self.changeState = ChangeState(self.config)
        self.repollCount = getattr(self.config.JobSubmitter, 'repollCount', 10000)

        # Steve additions
        self.cachedJobIDs = set()
        self.cachedJobs = {}
        self.jobsToPackage = {}

        self.packageDir = os.path.join(self.config.JobSubmitter.submitDir, "packages")
        if not os.path.exists(self.packageDir):
            os.makedirs(self.packageDir)

        self.listJobsAction = self.daoFactory(classname = "Jobs.ListForSubmitter")
        self.setLocationAction = self.daoFactory(classname = "Jobs.SetLocation")        
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
        jobPackage[loadedJob["id"]] = loadedJob

        batchID = self.jobsToPackage[loadedJob["workflow"]]["batchid"]
        
        if len(jobPackage.keys()) == 100:
            sandboxDir = os.path.dirname(jobPackage[jobPackage.keys()[0]]["sandbox"])
            batchDir = os.path.join(sandboxDir, "batch_%s" % batchID)

            if not os.path.exists(batchDir):
                os.makedirs(batchDir)
                
            batchPath = os.path.join(batchDir, "JobPackage.pkl")
            jobPackage.save(batchPath)
            del self.jobsToPackage[loadedJob["workflow"]]

        return batchID

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

        Query WMBS for all jobs in the "created" state.  For all jobs returned
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
        processedCount = 0
        for newJob in newJobs:
            jobCount += 1            
            if newJob["id"] in self.cachedJobIDs:
                continue

            processedCount += 1
            if processedCount % 5000 == 0:
                logging.info("Processed %d/%d new jobs." % (jobCount, len(newJobs)))

            pickledJobPath = os.path.join(newJob["cache_dir"], "job.pkl")
            jobHandle = open(pickledJobPath, "r")
            loadedJob = cPickle.load(jobHandle)

            possibleLocations = set(loadedJob["input_files"][0]["locations"])

            if len(loadedJob["siteWhitelist"]) > 0:
                possibleLocations = possibleLocations & set(loadedJob["siteWhitelist"])
            if len(loadedJob["siteBlacklist"]) > 0:
                possibleLocations = possibleLocations - set(loadedJob["siteBlacklist"])

            if len(possibleLocations) == 0:
                badJobs.append(newJob)
                continue

            batchID = self.addJobsToPackage(loadedJob)
            self.cachedJobIDs.add(newJob["id"])

            for possibleLocation in possibleLocations:
                if not self.cachedJobs.has_key(possibleLocation):
                    self.cachedJobs[possibleLocation] = {}
                if not self.cachedJobs[possibleLocation].has_key(newJob["type"]):
                    self.cachedJobs[possibleLocation][newJob["type"]] = set()
                    
                self.cachedJobs[possibleLocation][newJob["type"]].add((newJob["id"],
                                                                       newJob["retry_count"],
                                                                       batchID,                                                                       
                                                                       loadedJob["sandbox"],
                                                                       loadedJob["cache_dir"]))

        if len(badJobs) > 0:
            logging.error("The following jobs have no possible sites to run at: %s" % badJobs)
            self.changeState(badJobs, "submitfailed", "created")

        self.flushJobPackages()
        logging.info("Done.")
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
                if not submitThresholds.has_key(taskType):
                    submitThresholds[taskType]= []

                maxSlots = rcThresholds[siteName][taskType]["max_slots"]
                runningJobs = rcThresholds[siteName][taskType]["task_running_jobs"]                

                if runningJobs < maxSlots:
                    submitThresholds[taskType].append((rcThresholds[siteName][taskType]["se_name"],
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

        jobsToSubmit = set()
        jobsToPrune = set()

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
                while len(self.cachedJobs[emptySite[0]][taskType]) > 0:
                    cachedJob = self.cachedJobs[emptySite[0]][taskType].pop()

                    if cachedJob not in jobsToPrune:
                        self.cachedJobIDs.remove(cachedJob[0])                            
                        jobsToPrune.add(cachedJob)
                        jobsToSubmit.add((cachedJob[0], cachedJob[1], cachedJob[2],
                                          cachedJob[3], cachedJob[4], emptySite[0]))
                        break
                    else:
                        cachedJob = None

                # If we were able to pull down a job for the task/site and the
                # site it not full we'll still add more jobs to it.
                if emptySite[1] - 1 > 0 and cachedJob != None:
                    siteList.append((emptySite[0], emptySite[1] - 1))

        # Remove the jobs that we're going to submit from the cache.
        for siteName in self.cachedJobs.keys():
            for taskType in self.cachedJobs[siteName].keys():
                self.cachedJobs[siteName][taskType] -= jobsToPrune

        logging.info("Have %s jobs to submit." % len(jobsToSubmit))
        logging.info("Done assigning site locations.")
        return jobsToSubmit

    def algorithm(self, parameters = None):
        """
        _algorithm_

        """
        self.refreshCache()
        jobsToSubmit = self.assignJobLocations()

        idList = []
        for job in jobsToSubmit:
            idList.append({'jobid': job[0], 'location': job[5]})
        self.setLocationAction.execute(bulkList = idList)

        return

#        # Group the jobs by sandbox and then submit.
#             if len(sortedJobList[sandbox]) >= self.config.JobSubmitter.jobsPerWorker:
#                 self.processPool.enqueue([{'jobs': jobsReady,
#                                            'packageDir': packagePath,
#                                            'index': index,
#                                            'sandbox': sandbox,
#                                            'agentName': self.config.Agent.agentName}])



