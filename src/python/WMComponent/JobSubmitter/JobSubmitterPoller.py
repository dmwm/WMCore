#!/usr/bin/env python
#pylint: disable-msg=W0102, W6501, C0301
# W0102: We want to pass blank lists by default
# for the whitelist and the blacklist
# W6501: pass information to logging using string arguments
# C0301: I'm ignoring this because breaking up error messages is painful

"""
Creates jobs for new subscriptions

"""

__revision__ = "$Id: JobSubmitterPoller.py,v 1.33 2010/07/15 16:57:07 sfoulkes Exp $"
__version__ = "$Revision: 1.33 $"


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

class BadJobError(Exception):
    """
    Silly exception that doesn't do anything
    except signal

    """
    pass


class FullSitesError(Exception):
    """
    Another signal passing exception

    """

    def __init__(self, jobType):
        self.jobType = jobType

    pass


class BadJobCacheError(Exception):
    """
    Yet another signal passing exception

    """

    pass

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
                                       config = self.config, slaveInit = configDict)


        self.changeState = ChangeState(self.config)

        self.repollCount = getattr(self.config.JobSubmitter, 'repollCount', 10000)

        return

    def algorithm(self, parameters = None):
        """
        Actually runs the code
        """
        logging.debug("Running JSM.JobSubmitter")

        myThread = threading.currentThread()
        
        try:
            self.runSubmitter()
        except Exception, ex:
            msg = "Caught exception in JobSubmitter\n"
            msg += str(ex)
            msg += str(traceback.format_exc())
            msg += "\n\n"
            logging.error(msg)
            if hasattr(myThread, 'transaction') and myThread.transaction != None \
                   and hasattr(myThread.transaction, 'transaction') \
                   and myThread.transaction.transaction != None:
                myThread.transaction.rollback()
            raise Exception(msg)


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
        jobList = self.submitJobs(jobList)


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

        loadAction = self.daoFactory(classname = "Jobs.LoadForSubmitter")
        for jobType in self.types:


            results = loadAction.execute(type = jobType)

            if len(results) == 0:
                # Then we have no jobs of this type
                # Ignore!
                continue

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
                job['type']     = jobType
                newList.append(job)
                
        logging.info("Have %i jobs in JobSubmitter.getJobs()" % len(newList))

        return newList


    def pollJobs(self):
        """
        _pollJobs_
        
        Find the occupancy level of all sites
        """

        # Find types, we'll need them later
        logging.info("About to go find Subscription types in JobSubmitter.pollJobs()")
        typeFinder = self.daoFactory(classname = "Subscriptions.GetSubTypes")
        self.types = typeFinder.execute()
        logging.info("Found types in JobSubmitter.pollJobs()")

        self.sites = self.resourceControl.listThresholdsForSubmit()
        logging.info(self.sites)

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
            raise BadJobError


        # Now do the blacklist/whitelist magic
        # First we check the whitelist and if it has sites,
        # remove all sites now on whiteList
        whiteList = job.get('siteWhiteList', [])
        if len(whiteList) > 0:
            badList   = []
            for site in possibleSites:
                if not site in whiteList:
                    badList.append(site)
            if len(badList) > 0:
                for site in badList:
                    possibleSites.remove(site)
            if len(possibleSites) == 0:
                logging.error("Job %i has NO sites possible in whitelist" % (job['id']))
                raise BadJobError

        
        else:
            # If we don't have a whiteList, check for a blackList
            # Remove all blackList jobs
            for site in job.get('siteBlackList', []):
                if site in possibleSites:
                    possibleSites.remove(site)
            if len(possibleSites) == 0:
                logging.error("Job %i eliminated all sites in blacklist" % (job['id']))
                raise BadJobError

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
            #logging.error("Could not find site for job %i; all sites may be full" % (job['id']))
            sitesFull = True
            for siteName in self.sites.keys():
                site = self.sites[siteName][jobType]
                if site['task_running_jobs'] < site['max_slots']:
                    # Then at least one site has space...
                    sitesFull = False
                if sitesFull:
                    raise FullSitesError(jobType)

        return tmpSite



    def submitJobs(self, jobList):
        """
        _submitJobs_

        Grabs the task, sandbox, etc for each job by loading the pickled job
        from the JobCreator
        Then it submits them, splitting them into units and sending them to
        the plugins
        """

        failList = []
        jList2   = []
        killList = []
        fullList = []
        skippedJobs = 0
        noSiteJobs  = 0

        sortedJobList = {}
        submitIndex   = {}
        lenWork       = 0
        count         = 0

        for job in jobList:
            count += 1
            
            if job['type'] in fullList:
                # Then the sites are all full for that type
                # Skip that job
                skippedJobs += 1
                continue
            try:    
                loadedJob = self.loadJobFromPkl(job = job)
            except BadJobCacheError:
                killList.append(job)
                continue

            try:
                loadedJob['location'] = self.findSiteForJob(loadedJob)
                
            except BadJobError:
                # Really fail this job!  This means that something's
                # gone wrong in how sites were enabled.
                msg = ''
                msg += "Failing job %i: Encountered a job with no possible sites\n" % (job['id'])
                msg += "Job should enter submitFailed\n"
                logging.error(msg)
                killList.append(job)
                continue
            
            except FullSitesError as e:
                # If we've gotten here, we're in it deep
                # This means that all the sites are over their max limit
                msg = ''
                msg += 'All sites are full for type %s.\n' % (e.jobType)
                msg += 'No further jobs of that type will run this cycle\n'
                logging.error(msg)
                fullList.append(e.jobType)
                continue

            # If we didn't get a site, all sites are full
            if loadedJob['location'] == None:
                # Ignore this job until the next round
                noSiteJobs += 1
                continue

            
            loadedJob['custom']['location'] = loadedJob['location']
            loadedJob['retry_count'] = job['retry_count']
            if not 'sandbox' in loadedJob.keys() or not 'task' in loadedJob.keys():
                # You know what?  Just fail the job
                failList.append(loadedJob)
                continue

            # Sort jobs by sandbox
            sandbox = loadedJob['sandbox']
            if not sandbox in sortedJobList.keys():
                sortedJobList[sandbox] = []
            sortedJobList[sandbox].append(loadedJob)



            # Enqueue jobs
            if len(sortedJobList[sandbox]) >= self.config.JobSubmitter.jobsPerWorker:
                # First get them ready to run
                jobList = sortedJobList[sandbox]
                index = submitIndex.get(sandbox, 0)
                packagePath = os.path.join(os.path.dirname(sandbox),
                                           'batch_%i' %(jobList[0]['id']))
                jobsReady = self.prepForSubmit(jobList = jobList,
                                               sandbox = sandbox,
                                               packagePath = packagePath)
                self.processPool.enqueue([{'jobs': jobsReady,
                                           'packageDir': packagePath,
                                           'index': index,
                                           'sandbox': sandbox,
                                           'agentName': self.config.Agent.agentName}])


                # Increment our counters
                lenWork += 1
                if not sandbox in submitIndex.keys():
                    submitIndex[sandbox] = 0
                submitIndex[sandbox] += len(jobList)

                # We've submitted them, let's dump them
                sortedJobList[sandbox] = []


            jList2.append(loadedJob)

            # If we've run over enough jobs, repoll ResourceControl
            # This is to allow updated information during long polling cycles
            if count % self.repollCount == 0:
                self.pollJobs()
                # Now we have to fill the sites with
                # jobs that have not yet been submitted
                for sandbox in sortedJobList.keys():
                    for j in sortedJobList[sandbox]:
                        site = j['location']
                        self.sites[site][job['type']]['task_running_jobs'] += 1
                        for type in self.sites[site].keys():
                            self.sites[site][type]['total_running_jobs'] += 1

            

        # What happens we get to the end of the road?
        # Submit all unsubmitted jobs
        for sandbox in sortedJobList.keys():
            if len(sortedJobList[sandbox]) > 0:
                # Then we have to submit them.
                jobList = sortedJobList[sandbox]
                index = submitIndex.get(sandbox, 0)
                packagePath = os.path.join(os.path.dirname(sandbox),
                                           'batch_%i' %(jobList[0]['id']))
                jobsReady = self.prepForSubmit(jobList = jobList,
                                               sandbox = sandbox,
                                               packagePath = packagePath)
                self.processPool.enqueue([{'jobs': jobsReady,
                                           'packageDir': packagePath,
                                           'index': index,
                                           'sandbox': sandbox,
                                           'agentName': self.config.Agent.agentName}])
                lenWork += 1




        if skippedJobs > 0:
            logging.error("Skipped %i jobs because all sites were full." % (skippedJobs) )
        if noSiteJobs > 0:
            logging.error("Skipped %i jobs because we couldn't find a site for them" % (noSiteJobs) )


        # And then, at the end of the day, we have to dequeue them.
        result = []
        result = self.processPool.dequeue(lenWork)




        for job in failList:
            if job in jList2:
                jList2.remove(job)

        



        # Now dump the killed jobs
        # These are the jobs where it's impossible to run
        myThread = threading.currentThread()
        myThread.transaction.begin()
        self.changeState.propagate(killList, 'submitfailed', 'created')
        myThread.transaction.commit()


        
        return jList2


    def loadJobFromPkl(self, job):
        """
        _loadJobFromPkl_

        Load the job from the Pickle file and attach the right info
        """


        if not os.path.isdir(job['cache_dir']):
            # Well, then we're in trouble, because we need that info
            # Kill this job
            logging.error("Job %i has no cache directory." % (job['id']))
            raise BadJobCacheError

        jobPickle  = os.path.join(job['cache_dir'], 'job.pkl')
        if not os.path.isfile(jobPickle):
            # Then we don't have a pickle file, and we're screwed
            # Kill this job
            logging.error("Job %i has no pickled job file." % (job['id']))
            raise BadJobCacheError

        fileHandle = open(jobPickle, "r")
        loadedJob  = cPickle.load(fileHandle)
        loadedJob['type'] = job['type']

        return loadedJob


    def prepForSubmit(self, jobList, sandbox, packagePath):
        """
        _prepForSubmit_

        Prep a group of jobs for submission.  Get everything ready for ProcessPool
        """


        

        if not os.path.exists(packagePath):
            os.makedirs(packagePath)
        package = JobPackage()
        for job in jobList:
            package.append(job.getDataStructsJob())
            
        package.save(os.path.join(packagePath, 'JobPackage.pkl'))

        logging.info('About to repack jobs for Plugin')
        logging.info(len(jobList))


        # Now repack the jobs into a second list with only
        # Essential components
        finalList = []
        for job in jobList:
            tmpJob = Job(id = job['id'])
            tmpJob['custom']      = job['custom']
            #tmpJob['sandbox']     = job['sandbox']
            tmpJob['name']        = job['name']
            tmpJob['cache_dir']   = job['cache_dir']
            tmpJob['retry_count'] = job['retry_count']
            finalList.append(tmpJob)


        return finalList

    def terminate(self, params):
        """
        _terminate_
        
        Terminate code after final pass.
        """
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)


        


        




