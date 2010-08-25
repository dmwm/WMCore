#!/usr/bin/env python
"""
The actual jobTracker algorithm
"""
__all__ = []
__revision__ = "$Id: JobTrackerPoller.py,v 1.8 2010/05/03 19:24:22 mnorman Exp $"
__version__ = "$Revision: 1.8 $"

import threading
import logging
import os
import os.path

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.WMBS.Job          import Job
from WMCore.DAOFactory        import DAOFactory

from WMCore.JobStateMachine.ChangeState import ChangeState

class JobTrackerPoller(BaseWorkerThread):
    """
    Polls for Error Conditions, handles them
    """
    def __init__(self, config):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        self.config = config

        myThread = threading.currentThread()

        #I'm not entirely happy with this
        #But I think we need to keep global track of the failed jobs
        self.failedJobs    = {}
        self.runTimeLimit  = self.config.JobTracker.runTimeLimit
        self.idleTimeLimit = self.config.JobTracker.idleTimeLimit
        self.heldTimeLimit = self.config.JobTracker.heldTimeLimit
        self.unknTimeLimit = self.config.JobTracker.unknTimeLimit

        self.trackerInst   = None
        self.changeState   = None

        self.daoFactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)
    
    def setup(self, parameters):
        """
        Load DB objects required for queries
        """

        self.trackerInst = self.loadTracker()
        self.changeState = ChangeState(self.config)

        return




    def terminate(self, params):
        """
        _terminate_

        Terminate the function after one more run.
        """
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)
        return


    

    def algorithm(self, parameters):
        """
	Performs the archiveJobs method, looking for each type of failure
	And deal with it as desired.
        """
        logging.debug("Running algorithm for finding finished subscriptions")
        myThread = threading.currentThread()
        try:
            myThread.transaction.begin()
            self.trackJobs()
            myThread.transaction.commit()
        except:
            myThread.transaction.rollback()
            raise

        return

    def trackJobs(self):
        """
        _trackJobs_

        Finds a list of running jobs and the sites that they're running at,
        and passes that off to tracking.
        """

        jobListAction = self.daoFactory(classname = "Jobs.GetAllJobs")
        jobList       = jobListAction.execute(state = "Executing")

        if jobList == []:
            return
        
        jobDictList = []
        for job in jobList:
            jobDictList.append({'jobid': job})

        locListAction = self.daoFactory(classname = "Jobs.GetLocation")
        locDict = locListAction.execute(jobid = jobDictList)
        #logging.error("Have locations in trackJobs")
        #logging.error(locDict)

        trackDict = self.getInfo(locDict)

        #logging.error("Have info")
        #logging.info(trackDict)
        
        passedJobs, failedJobs = self.parseJobs(trackDict)

        #logging.error("In trackJobs")
        #logging.error(passedJobs)
        #logging.error(failedJobs)

        self.failJobs(failedJobs)
        self.passJobs(passedJobs)



        return

    def getInfo(self, jobDict):
        """
        _getInfo_

        Gets info on the individual jobs.  jobDict should be
        of form [{'id': id, 'site_name': site_name}]
        
        """
        trackerDict = self.trackerInst(jobDict)

        return trackerDict

    def parseJobs(self, trackDict):
        """
        _parseJobs_
        
        This parses the information that comes back from the tracker plugin
        The tracker plugin should report information in the following form.
        trackDict = {'jobName': {jobInfo}}
        jobInfo = {'Status': string, 'StatusTime': int since entering status,
        'StatusReason': string}
        """

        passedJobs = []
        failedJobs = []

        for job in trackDict.keys():
            if trackDict[job]['Status'] == 'Running' and trackDict[job]['StatusTime'] > self.runTimeLimit:
                failedJobs.append(job)
            elif trackDict[job]['Status'] == 'Idle' and trackDict[job]['StatusTime'] > self.idleTimeLimit:
                failedJobs.append(job)
            elif trackDict[job]['Status'] == 'Held' and trackDict[job]['StatusTime'] > self.heldTimeLimit:
                failedJobs.append(job)
            elif trackDict[job]['Status'] == 'Unknown' and trackDict[job]['StatusTime'] > self.unknTimeLimit:
                failedJobs.append(job)
            elif trackDict[job]['Status'] == 'NA':
                #Well, then we're not sure what happened to it.  Pass this on to the JobAccountant on
                #the assumption that it finished
                passedJobs.append(job)


        return passedJobs, failedJobs


    def failJobs(self, failedJobs):
        """
        _failJobs_

        Dump those jobs that have failed
        """
        if len(failedJobs) == 0:
            return
        
        #Kill them on the system
        self.trackerInst.kill(failedJobs)

        #Mark 'em as failed
        listOfJobs = []
        for jobID in failedJobs:
            job = Job(id = jobID)
            job.load()
            listOfJobs.append(job)
            job.setFWJRPath(os.path.join(job.getCache(),
                                         'Report.%i.pkl' % (job['retry_count'])))

        self.changeState.propagate(listOfJobs, 'jobfailed', 'executing')


        return

    def passJobs(self, passedJobs):
        """
        _passJobs_
        
        Pass jobs and move their stuff?
        """

        if len(passedJobs) == 0:
            return

        myThread = threading.currentThread()

        #Get their stuff?
        #I've got no idea how we want to do this.
        
        #Mark 'em as complete
        listOfJobs = []
        for jobID in passedJobs:
            job = Job(id = jobID)
            job.load()
            listOfJobs.append(job)
            job.setFWJRPath(os.path.join(job.getCache(), \
                                         'Report.pkl'))
            #logging.error("Job %i has finished on site" %(jobID))

        myThread.transaction.begin()
        logging.error("Propagating jobs in jobTracker")
        #logging.error(listOfJobs)
        self.changeState.propagate(listOfJobs, 'complete', 'executing')
        myThread.transaction.commit()

        return
                
    


    def loadTracker(self, trackerName = None):
        """
        Gets the tracker plugin and returns an instance of that tracker. 
        Pluging should be triggered through call()
        """

        if not trackerName:
            trackerName = self.config.JobTracker.trackerName

        pluginDir  = self.config.JobTracker.pluginDir

        pluginPath = pluginDir + '.' + trackerName

        try:
            module      = __import__(pluginPath, globals(), \
                                     locals(), [trackerName])
            instance    = getattr(module, trackerName)
            loadedClass = instance(self.config)
            #print loadedClass.__class__
        except Exception, ex:
            msg = "Failed in attempting to import tracker plugin in JobTracker with msg = \n%s" % (ex)
            raise Exception(msg)
        

        return loadedClass


    def killJobs(self, jobList = []):
        """
        Uses the tracker plugin to kill jobs

        """
        self.trackerInst.getClassAds()
        self.trackerInst.kill(jobList)

        return

        
