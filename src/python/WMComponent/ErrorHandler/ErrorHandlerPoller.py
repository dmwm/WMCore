#!/usr/bin/env python
#pylint: disable-msg=W0613
"""
The actual error handler algorithm
"""
__all__ = []
__revision__ = "$Id: ErrorHandlerPoller.py,v 1.3 2009/07/28 21:27:38 mnorman Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "anzar@fnal.gov"

import threading
import logging
import re
from sets import Set

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Fileset      import Fileset
from WMCore.WMBS.Workflow     import Workflow
from WMCore.WMBS.Job          import Job
from WMCore.WMFactory         import WMFactory
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
    
    def setup(self, parameters):
        """
        Load DB objects required for queries
        """

        myThread = threading.currentThread()

        self.daofactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)
        self.changeState = ChangeState(self.config)

        self.maxRetries = self.config.ErrorHandler.maxRetries


    def processRetries(self, jobs, type):
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
                logging.error("Job %i had %s retries remaining" %(ajob['id'], str(ajob['retry_count'])))

        #Now to actually do something.

        self.changeState.propagate(exhaustJobs, 'exhausted', '%sfailed' %(type))
        self.changeState.propagate(cooloffJobs, '%scooloff' %(type), '%sfailed' %(type))

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
        logging.debug("Found %s failed jobs failed during creation" % len(idList))
        for id in idList:
            job = Job(id = id)
            job.loadData()
            createList.append(job)
        idList = getJobs.execute(state = 'SubmitFailed')
        logging.debug("Found %s failed jobs failed during submit" % len(idList))
        for id in idList:
            job = Job(id = id)
            job.loadData()
            submitList.append(job)
        idList = getJobs.execute(state = 'JobFailed')
        logging.debug("Found %s failed jobs failed during execution" % len(idList))
        for id in idList:
            job = Job(id = id)
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
