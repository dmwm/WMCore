#!/usr/bin/env python
#pylint: disable-msg=W0102, W6501
# W0102: We want to pass blank lists by default
# for the whitelist and the blacklist
# W6501: pass information to logging using string arguments

"""
Creates jobs for new subscriptions

"""

__revision__ = "$Id: JobSubmitterPoller.py,v 1.28 2010/06/23 20:40:34 mnorman Exp $"
__version__ = "$Revision: 1.28 $"


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

# WMBS objects
from WMCore.WMBS.Job          import Job
from WMCore.DAOFactory        import DAOFactory

from WMCore.JobStateMachine.ChangeState       import ChangeState
from WMCore.WorkerThreads.BaseWorkerThread    import BaseWorkerThread
from WMCore.ProcessPool.ProcessPool           import ProcessPool
from WMCore.ResourceControl.ResourceControl   import ResourceControl
from WMCore.DataStructs.JobPackage            import JobPackage
from WMCore.WMBase        import getWMBASE

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

        self.session = None
        self.schedulerConfig = {}
        self.config = config
        self.types = []

        #Libraries
        self.resourceControl = ResourceControl()

        BaseWorkerThread.__init__(self)

        configDict = {"submitDir": self.config.JobSubmitter.submitDir,
                      "submitNode": self.config.JobSubmitter.submitNode}
        
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
            if hasattr(myThread, 'transaction') and myThread.transaction != None \
                   and hasattr(myThread.transaction, 'transaction') \
                   and myThread.transaction.transaction != None:
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

        getJobs    = self.daoFactory(classname = "Jobs.GetAllJobs")
        loadAction = self.daoFactory(classname = "Jobs.LoadFromID")
        for jobType in self.types:
            jobList   = getJobs.execute(state = 'Created', jobType = jobType)

            if len(jobList) == 0:
                continue

            binds = []
            for jobID in jobList:
                binds.append({"jobid": jobID})

            results = loadAction.execute(jobID = binds)

            # You have to have a list
            if type(results) == dict:
                results = [results]

            listOfJobs = []
            for entry in results:
                # One job per entry
                tmpJob = Job(id = entry['id'])
                tmpJob.update(entry)
                listOfJobs.append(tmpJob)

            for job in listOfJobs:
                #job.getMask()
                job['type']     = jobType
                #job['location'] = self.findSiteForJob(job)
                #if not job['location']:
                    # Then all sites are full for this round
                    # Ignore this job until later
                #    continue
                #job['custom']['location'] = job['location']    #Necessary for JSON
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

        # Get locations for job
        # All files SHOULD be alike, this is a prerequisite
        # of the entire WMAgent system
        availableSites = []
        if len(job['input_files']) == 0:
            # Then it has no files
            # Can run anywhere?
            availableSites = []
            for site in self.sites.keys():
                if jobType in self.sites[site].keys():
                    availableSites.append(self.sites[site][jobType]['se_name'])
        else:
            for site in job['input_files'][0]['locations']:
                availableSites.append(site)


        # You want only sites that are BOTH in keys and availableSites
        # and that allow that jobType
        possibleSites = []
        for site in self.sites.keys():
            if not jobType in self.sites[site].keys():
                continue
            if not self.sites[site][jobType]['se_name'] in availableSites:
                continue
            possibleSites.append(site)

        if len(possibleSites) == 0:
            logging.error("Job %i has NO possible sites" % (job['id']))
            return None


        # Now do the blacklist/whitelist magic
        # First we check the whitelist and if it has sites,
        # remove all sites now on whiteList
        if len(job.get('siteWhiteList', [])) > 0:
            for site in possibleSites:
                if not site in job['siteWhiteList']:
                    possibleSites.remove(site)
            if len(possibleSites) == 0:
                logging.error("Job %i has NO sites possible in whitelist" % (job['id']))
                return None
        
        else:
            # If we don't have a whiteList, check for a blackList
            # Remove all blackList jobs
            for site in job.get('siteBlackList', []):
                if site in possibleSites:
                    possibleSites.remove(site)
            if len(possibleSites) == 0:
                logging.error("Job %i eliminated all sites in blacklist" % (job['id']))
                return None



        

        # First look for sites where we have
        # less then the minimum jobs of this type
        for site in possibleSites:
            siteDict = self.sites[site][jobType]
            if not siteDict['task_running_jobs'] < siteDict['max_slots']:
                # Then we have too many jobs in this place already
                continue
            nSpaces = self.sites[site][jobType]['min_slots'] \
                      - self.sites[site][jobType]['task_running_jobs']
            if nSpaces > tmpSlots:
                tmpSlots = nSpaces
                tmpSite  = site
        if tmpSlots < 0:  # Then we didn't have any sites under the minimum
            tmpSlots = -999999
            tmpSite  = None
            for site in possibleSites:
                siteDict = self.sites[site][jobType]
                if not siteDict['task_running_jobs'] < siteDict['max_slots']:
                    # Then we have too many jobs for this task
                    # Ignore this site
                    continue
                nSpaces = siteDict['total_slots'] - siteDict['total_running_jobs']
                if nSpaces > tmpSlots:
                    tmpSlots = nSpaces
                    tmpSite  = site

        # Having chosen a site, account for it
        if tmpSite:
            self.sites[tmpSite][jobType]['task_running_jobs'] += 1
            for key in self.sites[tmpSite].keys():
                self.sites[tmpSite][key]['total_running_jobs'] += 1
        else:
            logging.error("Could not find site for job %i; all sites may be full" % (job['id']))


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

            # Now repack the jobs into a second list with only
            # Essential components
            finalList = []
            for job in listOfJobs:
                tmpJob = Job(id = job['id'])
                tmpJob['custom']      = job['custom']
                #tmpJob['sandbox']     = job['sandbox']
                tmpJob['name']        = job['name']
                tmpJob['cache_dir']   = job['cache_dir']
                tmpJob['retry_count'] = job['retry_count']
                finalList.append(tmpJob)


            # We need to increment an index so we know what
            # number job we're submitting
            index = 0





            while len(finalList) > self.config.JobSubmitter.jobsPerWorker:
                listForSub = finalList[:self.config.JobSubmitter.jobsPerWorker]
                finalList = finalList[self.config.JobSubmitter.jobsPerWorker:]
                self.processPool.enqueue([{'jobs': listForSub,
                                           'packageDir': packagePath,
                                           'index': index,
                                           'sandbox': sandbox,
                                           'agentName': self.config.Agent.agentName}])
                count += 1
                index += len(listForSub)
                
            if len(finalList) > 0:
                self.processPool.enqueue([{'jobs': finalList,
                                           'packageDir': packagePath,
                                           'index': index,
                                           'sandbox': sandbox,
                                           'agentName': self.config.Agent.agentName}])
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
        jList2   = []

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
            loadedJob['type'] = job['type']
            loadedJob['location'] = self.findSiteForJob(loadedJob)
            loadedJob['custom']['location'] = loadedJob['location']
            # If we didn't get a site, all sites are full
            if loadedJob['location'] == None:
                # Ignore this job until the next round
                failList.append(loadedJob)
                continue
            loadedJob['retry_count'] = job['retry_count']
            jList2.append(loadedJob)
            if not 'sandbox' in loadedJob.keys() or not 'task' in loadedJob.keys():
                # You know what?  Just fail the job
                failList.append(loadedJob)
                continue


        for job in jList2:
            if job in failList:
                jList2.remove(job)

        return jList2

    def terminate(self, params):
        """
        _terminate_
        
        Terminate code after final pass.
        """
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)


        


        




