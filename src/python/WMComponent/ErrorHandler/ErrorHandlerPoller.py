#!/usr/bin/env python
#pylint: disable-msg=W0613
"""
The actual error handler algorithm
"""
__all__ = []
__revision__ = "$Id: ErrorHandlerPoller.py,v 1.2 2009/05/12 11:50:55 afaq Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "anzar@fnal.gov"

import threading
import logging
import re
from sets import Set

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.WMFactory import WMFactory

from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Workflow import Workflow

class ErrorHandlerPoller(BaseWorkerThread):
    """
    Polls for Error Conditions, handles them
    """
    def __init__(self):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)
    
    def setup(self, parameters):
        """
        Load DB objects required for queries
        """
        myThread = threading.currentThread()
        factory = WMFactory("default", \
            "WMComponent.ErrorHandler.Database." + myThread.dialect)

	self.failedcreatequery = factory.loadObject("FindFailedCreates")
	self.failedsubmitquery = factory.loadObject("FindFailedSubmits")
	self.failedjobquery = factory.loadObject("FindFailedJobs")


    def processRetries(self, jobs, type):

	# Retries < max retry count
        for ajob in jobs:
                # Retries < max retry count
                if ajob['retry_count'] < ajob['retry_max']:
                        #SIMON's CODE SHOULD PUT the job in "exhausted" state
                # Check if Retries >= max retry count
                if ajob['retry_count'] >= ajob['retry_max']:
			newstate=type+'cooloff'
                        #SIMON's CODE SHOULD PUT the job in "newstate" state

    def handleErrors(self):
        """
        Queries DB for all watched filesets, if matching filesets become
        available, create the subscriptions
        """
        # Discover the jobs that failed in create step (with status 'createfailed')
        jobs = self.failedcreatequery.execute()
        logging.debug("Found %s failed jobs failed during creation" % len(jobs))
	processRetries(self, jobs, 'create')

         # Discover the jobs that failed in submit step (with status 'submitfailed')
        jobs = self.failedsubmitquery.execute()
        logging.debug("Found %s failed jobs failed during submit" % len(jobs))
	processRetries(self, jobs, 'submit')

	# Discover the jobs that failed in run step (with status 'jobfailed')
        jobs = self.failedjobquery.execute()
        logging.debug("Found %s failed jobs failed during execution" % len(jobs))
	processRetries(self, jobs, 'job')

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
