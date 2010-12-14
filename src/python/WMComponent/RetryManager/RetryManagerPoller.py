#!/usr/bin/env python
#pylint: disable-msg=W0613, W6501
"""
The actual retry algorithm(s)
"""
__all__ = []



import threading
import logging
import datetime
import time
import os
import os.path
import traceback

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.WMBS.Job          import Job
from WMCore.DAOFactory        import DAOFactory
from WMCore.WMFactory         import WMFactory

from WMCore.JobStateMachine.ChangeState import ChangeState
from WMCore.JobStateMachine.Transitions import Transitions

from WMCore.WMException import WMException

def convertdatetime(t):
    """
    Convert dates into useable format.
    """
    return int(time.mktime(t.timetuple()))

def timestamp():
    """
    generate a timestamp
    """
    t = datetime.datetime.now()
    return convertdatetime(t)

class RetryManagerException(WMException):
    """
    _RetryManagerException_

    It's totally awesome, except it's not.
    """

class RetryManagerPoller(BaseWorkerThread):
    """
    _RetryManagerPoller_
    
    Polls for Jobs in CoolOff State and attempts to retry them
    based on the requirements in the selected plugin
    """
    def __init__(self, config):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        self.config = config

        myThread = threading.currentThread()

        self.daoFactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)

        pluginPath = getattr(self.config.RetryManager, "pluginPath",
                             "WMComponent.RetryManager.PlugIns")
        self.pluginFactory = WMFactory("plugins", pluginPath)
        
        self.changeState = ChangeState(self.config)
        self.getJobs     = self.daoFactory(classname = "Jobs.GetAllJobs")

        try:
            pluginName  = getattr(self.config.RetryManager, 'pluginName', 'DefaultRetryAlgo')
            self.plugin = self.pluginFactory.loadObject(classname = pluginName,
                                                        args = config)
        except Exception, ex:
            msg =  "Error loading plugin %s on path %s\n" % (pluginName, pluginPath)
            msg += str(ex)
            logging.error(msg)
            raise RetryManagerException(msg)

        return

    def terminate(self, params):
        """
        Run one more time through, then terminate

        """
        logging.debug("Terminating. doing one more pass before we die")
        self.algorithm(params)


    def algorithm(self, parameters = None):
        """
	Performs the doRetries method, loading the appropriate
        plugin for each job and handling it.
        """
        logging.debug("Running retryManager algorithm")
        myThread = threading.currentThread()
        try:
            myThread.transaction.begin()
            self.doRetries()
            myThread.transaction.commit()
        except WMException, ex:
            if getattr(myThread, 'transaction', None) and \
               getattr(myThread.transaction, 'transaction', None):
                myThread.transaction.rollback()
            raise
        except Exception, ex:
            msg = "Caught exception in RetryManager\n"
            msg += str(ex)
            msg += str(traceback.format_exc())
            msg += "\n\n"
            logging.error(msg)
            if hasattr(myThread, 'transaction') \
                   and myThread.transaction != None \
                   and hasattr(myThread.transaction, 'transaction') \
                   and myThread.transaction.transaction != None:
                myThread.transaction.rollback()
            raise Exception(msg)


    def processRetries(self, jobs, jobType):
        """
        _processRetries_
        
        Actually does the dirty work of figuring out what to do with jobs
        """

        if len(jobs) < 1:
            # We got no jobs?
            return

        transitions = Transitions()
        oldstate = '%scooloff' % (jobType)
        if not oldstate in transitions.keys():
            logging.error('Unknown job type %s' % (jobType))
            return
        propList = []

        newJobType  = transitions[oldstate][0]

        jobList = self.loadJobsFromList(idList = jobs)
    
        # Now we should have the jobs
        propList = self.selectRetryAlgo(jobList, jobType)

        if len(propList) > 0:
            self.changeState.propagate(propList, newJobType, oldstate)


        return


    def loadJobsFromList(self, idList):
        """
        _loadJobsFromList_

        Load jobs in bulk
        """

        loadAction = self.daoFactory(classname = "Jobs.LoadFromID")


        binds = []
        for jobID in idList:
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


        return listOfJobs


    def selectRetryAlgo(self, jobList, jobType):
        """
        _selectRetryAlgo_

        Selects which retry algorithm to use
        """
        result = []

        if len(jobList) == 0:
            return result


        for job in jobList:
            try:
                if self.plugin.isReady(job = job, jobType = jobType):
                    result.append(job)
            except Exception, ex:
                msg =  "Exception while checking for cooloff timeout for job %i\n" % job['id']
                msg += str(ex)
                logging.error(msg)
                logging.debug("Job: %s\n" % job)
                logging.debug("jobType: %s\n" % jobType)
                raise RetryManagerException(msg)
            

        return result

    def doRetries(self):
        """
        Queries DB for all watched filesets, if matching filesets become
        available, create the subscriptions
        """
        # Discover the jobs that are in create cooloff
        jobs = self.getJobs.execute(state = 'createcooloff')
        logging.info("Found %s jobs in createcooloff" % len(jobs))
        self.processRetries(jobs, 'create')

         # Discover the jobs that are in submit cooloff
        jobs = self.getJobs.execute(state = 'submitcooloff')
        logging.info("Found %s jobs in submitcooloff" % len(jobs))
        self.processRetries(jobs, 'submit')

	# Discover the jobs that are in run cooloff
        jobs = self.getJobs.execute(state = 'jobcooloff')
        logging.info("Found %s jobs in jobcooloff" % len(jobs))
        self.processRetries(jobs, 'job')
