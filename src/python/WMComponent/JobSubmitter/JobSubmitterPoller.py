#!/usr/bin/env python
#pylint: disable-msg=W0102, W6501
# W0102: We want to pass blank lists by default
# for the whitelist and the blacklist
# W6501: pass information to logging using string arguments

"""
Creates jobs for new subscriptions

"""

__revision__ = "$Id: JobSubmitterPoller.py,v 1.15 2010/03/03 19:24:15 mnorman Exp $"
__version__ = "$Revision: 1.15 $"


#This job currently depends on the following config variables in JobSubmitter:
# pluginName
# pluginDir

import logging
import threading
import time
import os.path
import cPickle
import random
import traceback
#import common

# WMBS objects
from WMCore.WMBS.Job          import Job
from WMCore.DAOFactory        import DAOFactory

#from WMCore.WMSpec.WMWorkload                 import WMWorkload, WMWorkloadHelper
from WMCore.JobStateMachine.ChangeState       import ChangeState
from WMCore.WorkerThreads.BaseWorkerThread    import BaseWorkerThread
from WMCore.ProcessPool.ProcessPool           import ProcessPool
from WMCore.ResourceControl.ResourceControl   import ResourceControl
from WMCore.DataStructs.JobPackage            import JobPackage


def sortListOfDictsByKey(inList, key):
    """
    Sorts a list of dictionaries into a dictionary by keys

    """

    finalList = {}

    for entry in inList:
        value = entry.get(key, '__NoKey__')
        if not value in finalList.keys():
            finalList[value] = []
        finalList[value].append(entry)

    return finalList



class JobSubmitterPoller(BaseWorkerThread):
    """
    Handles job submission

    """
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

        #Set config objects
        self.database = config.CoreDatabase.connectUrl

        (connectDialect, junk) = config.CoreDatabase.connectUrl.split(":", 1)
        if connectDialect.lower() == "mysql":
            self.dialect = "MySQL"
        elif connectDialect.lower() == "oracle":
            self.dialect = "Oracle"
        elif connectDialect.lower() == "sqlite":
            self.dialect = "SQLite"
        
        self.session = None
        self.schedulerConfig = {}
        self.config = config
        self.types = []

        #Libraries
        self.resourceControl = ResourceControl()

        BaseWorkerThread.__init__(self)

        configDict = {'submitDir': self.config.JobSubmitter.submitDir, \
                      'submitNode': self.config.JobSubmitter.submitNode,
                      'submitScript': self.config.JobSubmitter.submitScript}
        if hasattr(self.config.JobSubmitter, 'inputFile'):
            configDict['inputFile'] = self.config.JobSubmitter.inputFile

        workerName = "%s.%s" % (self.config.JobSubmitter.pluginDir, \
                                self.config.JobSubmitter.pluginName)

        self.processPool = ProcessPool(workerName,
                                       totalSlaves = self.config.JobSubmitter.workerThreads,
                                       componentDir = self.config.JobSubmitter.componentDir,
                                       config = self.config, slaveInit = configDict)

        return

    def algorithm(self, parameters = None):
        """
        Actually runs the code
        """
        logging.debug("Running JSM.JobSubmitter")

        myThread = threading.currentThread()
        
        try:
            #startTime = time.clock()
            self.runSubmitter()
            #stopTime = time.clock()
            #logging.debug("Running jobSubmitter took %f seconds" \
            #              %(stopTime - startTime))
            #print "Runtime for JobSubmitter %f" %(stopTime - startTime)
            #print self.timing
        except Exception, ex:
            msg = "Caught exception in JobSubmitter\n"
            msg += str(ex)
            msg += str(traceback.format_exc())
            msg += "\n\n"
            if hasattr(myThread, 'transaction'):
                myThread.transaction.rollback()
            raise


    def runSubmitter(self):
        """
        _runSubmitter_

        Keeps track of, and does, everything
        """

        logging.info("About to call JobSubmitter.pollJobs()")
        self.pollJobs()
        if not len(self.sites.keys()) > 0:
            # Then we have no active sites?
            # Return!
            return
        jobList = self.getJobs()
        jobList = self.grabTask(jobList)
        self.submitJobs(jobList)


        idList = []
        for job in jobList:
            idList.append({'jobid': job['id'], 'location': job['location']})
        setLocationAction = self.daoFactory(classname = "Jobs.SetLocation")
        setLocationAction.execute(bulkList = idList)

        return


    def getJobs(self):
        """
        _getJobs_

        This uses WMBS to extract a list of jobs in the 'Created' state
        """
        newList = []

        getJobs = self.daoFactory(classname = "Jobs.GetAllJobs")
        for jobType in self.types:
            jobList   = getJobs.execute(state = 'Created', jobType = jobType)
            for jobID in jobList:
                job = Job(id = jobID)
                job.load()
                job['type']     = jobType
                job["location"] = self.findSiteForJob(job)
                if not job['location']:
                    # Then all sites are full for this round
                    # Ignore this job until later
                    continue
                job['custom']['location'] = job['location']    #Necessary for JSON
                # Take care of accounting for the job
                #self.sites[job['location']][jobType]['task_running_jobs'] += 1
                #for key in self.sites[job['location']].keys():
                #    self.sites[job['location']][key]['total_running_jobs'] += 1
                # Now add the new job
                newList.append(job)

        logging.info("Have %i jobs in JobSubmitter.getJobs()" % len(newList))

        return newList


    def pollJobs(self):
        """
        _pollJobs_
        
        Find the occupancy level of all sites
        """

        # Find types, we'll need them later
        logging.error("About to go find Subscription types in JobSubmitter.pollJobs()")
        typeFinder = self.daoFactory(classname = "Subscriptions.GetSubTypes")
        self.types = typeFinder.execute()
        logging.error("Found types in JobSubmitter.pollJobs()")

        self.sites = self.resourceControl.listThresholdsForSubmit()
        logging.error(self.sites)

        return


    def findSiteForJob(self, job):
        """
        _findSiteForJob_
        
        Find a site for the job to run at based on information from ResourceControl
        This is the most complicated portion of the code
        """

        jobType = job['type']

        tmpSlots = -999999
        tmpSite  = None

        # First look for sites where we have
        # less then the minimum jobs of this type
        for site in self.sites.keys():
            if not jobType in self.sites[site].keys():
                # Then we don't actually have this type at this site
                continue
            nSpaces = self.sites[site][jobType]['min_slots'] \
                      - self.sites[site][jobType]['task_running_jobs']
            if nSpaces > tmpSlots:
                tmpSlots = nSpaces
                tmpSite  = site
        if tmpSlots < 0:  # Then we didn't have any sites under the minimum
            for site in self.sites.keys():
                tmpSlots = -999999
                tmpSite  = None
                if not jobType in self.sites[site].keys():
                    # Then we don't actually have this type at this site
                    continue
                siteDict = self.sites[site][jobType]
                if not siteDict['task_running_jobs'] < siteDict['max_slots']:
                    # Then we have too many jobs for this task
                    # Ignore this site
                    continue
                nSpaces = siteDict['total_slots'] - siteDict['total_running_jobs']
                if nSpaces > tmpSlots:
                    tmpSlots = nSpaces
                    tmpSite  = site

        # I guess we shouldn't do this
        # if not tmpSite:
            # The chances of this are so ludicrously
            # low that it should never happen
            # Nevertheless, having said that...
            # Randomly choose a site
        #    tmpSite = random.choice(self.sites.keys())

        # Having chosen a site, account for it
        if tmpSite:
            self.sites[tmpSite][jobType]['task_running_jobs'] += 1
            for key in self.sites[tmpSite].keys():
                self.sites[tmpSite][key]['total_running_jobs'] += 1


        return tmpSite


    def submitJobs(self, jobList):
        """
        _submitJobs_
        
        This runs over the list of jobs and submits them all
        """

        myThread = threading.currentThread()

        sortedJobList = sortListOfDictsByKey(jobList, 'sandbox')

        changeState = ChangeState(self.config)

        logging.error("In submitJobs")
        logging.error(len(jobList))

        count = 0
        successList = []
        failList    = []
        for sandbox in sortedJobList.keys():
            if not sandbox or not os.path.isfile(sandbox):
                #Sandbox does not exist!  Dump jobs!
                for job in sortedJobList[sandbox]:
                    failList.append(job)
            listOfJobs = sortedJobList[sandbox][:]
            packagePath = os.path.join(os.path.dirname(sandbox),
                                       'batch_%i' %(listOfJobs[0]['id']))
            if not os.path.exists(packagePath):
                os.makedirs(packagePath)
            package = JobPackage()
            for job in listOfJobs:
                package.append(job.getDataStructsJob())
            #package.extend(listOfJobs)
            package.save(os.path.join(packagePath, 'JobPackage.pkl'))

            logging.error('About to send jobs to Plugin')
            logging.error(len(listOfJobs))
            
            while len(listOfJobs) > self.config.JobSubmitter.jobsPerWorker:
                listForSub = listOfJobs[:self.config.JobSubmitter.jobsPerWorker]
                listOfJobs = listOfJobs[self.config.JobSubmitter.jobsPerWorker:]
                self.processPool.enqueue([{'jobs': listForSub,
                                           'packageDir': packagePath}])
                count += 1
            if len(listOfJobs) > 0:
                self.processPool.enqueue([{'jobs': listOfJobs,
                                           'packageDir': packagePath}])
                count += 1

        #result = self.processPool.dequeue(len(jobList))
        result = []
        #for i in range(0, count):
        result = self.processPool.dequeue(count)

        #This will return a list of dictionaries of job ids
             
        successCompilation = []
        for entry in result:
            if 'Success' in entry.keys():
                successCompilation.extend(entry['Success'])

        for job in jobList:
            if job['id'] in successCompilation:
                successList.append(job)
            else:
                failList.append(job)

        #Pass the successful jobs, and fail the bad ones
        myThread.transaction.begin()
        changeState.propagate(successList, 'executing',    'created')
        changeState.propagate(failList,    'submitfailed', 'created')

        myThread.transaction.commit()

        return


    def grabTask(self, jobList):
        """
        _grabTask_

        Grabs the task, sandbox, etc for each job by using the WMBS DAO object
        """

        failList = []

        for job in jobList:
            if not os.path.isdir(job['cache_dir']):
                #Well, then we're in trouble, because we need that info
                failList.append(job)
                continue
            jobPickle  = os.path.join(job['cache_dir'], 'job.pkl')
            if not os.path.isfile(jobPickle):
                failList.append(job)
                continue
            fileHandle = open(jobPickle, "r")
            loadedJob  = cPickle.load(fileHandle)
            for key in loadedJob.keys():
                if loadedJob[key]:
                    job[key] = loadedJob[key]
            if not 'sandbox' in job.keys() or not 'task' in job.keys():
                # You know what?  Just fail the job
                failList.append(job)
                continue
                #Then we need to construct a task or a sandbox
                #if not 'spec' in job.keys():
                #    #Well, we have no spec
                #    failList.append(job)
                #    continue
                #if not os.path.isfile(job['spec']):
                #    failList.append(job)
                #    continue
                #wmWorkload = WMWorkloadHelper(WMWorkload("workload"))
                #wmWorkload.load(job['spec'])
                #job['sandbox'] = task.data.input.sandbox

        for job in jobList:
            if job in failList:
                jobList.remove(job)

        return jobList

    def terminate(self, params):
        """
        _terminate_
        
        Terminate code after final pass.
        """
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)


        


        




