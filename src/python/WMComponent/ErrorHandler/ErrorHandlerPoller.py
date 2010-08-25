#!/usr/bin/env python
#pylint: disable-msg=W0613, W6501
# W6501: It doesn't like string formatting in logging messages
"""
The actual error handler algorithm
"""
__all__ = []
__revision__ = "$Id: ErrorHandlerPoller.py,v 1.7 2010/04/29 14:49:54 mnorman Exp $"
__version__ = "$Revision: 1.7 $"

import threading
import logging

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.WMBS.Job          import Job
from WMCore.DAOFactory        import DAOFactory

from WMCore.JobStateMachine.ChangeState import ChangeState

class ErrorHandlerPoller(BaseWorkerThread):
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

        self.daofactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)
        self.changeState = ChangeState(self.config)

        self.maxRetries = self.config.ErrorHandler.maxRetries

        return
    
    def setup(self, parameters):
        """
        Load DB objects required for queries
        """

        # For now, does nothing

        return

        

    def terminate(self, params):
        """
        _terminate_
        
        Do one pass, then commit suicide
        """
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)


    def processRetries(self, jobs, jobType):
        """
        Actually do the retries

        """

        exhaustJobs = []
        cooloffJobs = []

	# Retries < max retry count
        for ajob in jobs:
            # Retries < max retry count
            if ajob['retry_count'] < self.maxRetries:
                cooloffJobs.append(ajob)
            # Check if Retries >= max retry count
            if ajob['retry_count'] >= self.maxRetries:
                exhaustJobs.append(ajob)
                #SIMON's CODE SHOULD PUT the job in "newstate" state
            else:
                logging.error("Job %i had %s retries remaining" \
                              %(ajob['id'], str(ajob['retry_count'])))

        #Now to actually do something.

        self.changeState.propagate(exhaustJobs, 'exhausted', \
                                   '%sfailed' %(jobType))
        self.changeState.propagate(cooloffJobs, '%scooloff' %(jobType), \
                                   '%sfailed' %(jobType))

        # Remove all the files in the exhausted jobs.
        for job in exhaustJobs:
            job.failInputFiles()

    def handleErrors(self):
        """
        Queries DB for all watched filesets, if matching filesets become
        available, create the subscriptions
        """

        createList = []
        submitList = []
        jobList    = []

        getJobs = self.daofactory(classname = "Jobs.GetAllJobs")

        idList = getJobs.execute(state = 'CreateFailed')
        logging.debug("Found %s failed jobs failed during creation" \
                      % len(idList))
        for jid in idList:
            job = Job(id = jid)
            job.loadData()
            createList.append(job)
        idList = getJobs.execute(state = 'SubmitFailed')
        logging.debug("Found %s failed jobs failed during submit" \
                      % len(idList))
        for jid in idList:
            job = Job(id = jid)
            job.loadData()
            submitList.append(job)
        idList = getJobs.execute(state = 'JobFailed')
        logging.debug("Found %s failed jobs failed during execution" \
                      % len(idList))
        for jid in idList:
            job = Job(id = jid)
            job.loadData()
            jobList.append(job)


        self.processRetries(createList, 'create')
        self.processRetries(submitList, 'submit')
        self.processRetries(jobList, 'job')


    def algorithm(self, parameters):
        """
	Performs the handleErrors method, looking for each type of failure
	And deal with it as desired.
        """
        logging.debug("Running subscription / fileset matching algorithm")
        myThread = threading.currentThread()
        try:
            myThread.transaction.begin()
            self.handleErrors()
            myThread.transaction.commit()
        except:
            myThread.transaction.rollback()
            raise
