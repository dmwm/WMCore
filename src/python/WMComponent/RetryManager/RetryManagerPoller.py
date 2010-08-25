#!/usr/bin/env python
#pylint: disable-msg=W0613
"""
The actual retry algorithm(s)
"""
__all__ = []
__revision__ = "$Id: RetryManagerPoller.py,v 1.1 2009/05/12 11:52:35 afaq Exp $"
__version__ = "$Revision: 1.1 $"
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

class RetryMangerPoller(BaseWorkerThread):
    """
    Polls for Jobs in CoolOff State and attempts to retry them
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

	self.createcooloffquery = factory.loadObject("FindCooloffCreates")
	self.submitcooloffquery = factory.loadObject("FindCooloffSubmits")
	self.jobcooloffquery = factory.loadObject("FindCooloffJobs")


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

    def deRetries(self):
        """
        Queries DB for all watched filesets, if matching filesets become
        available, create the subscriptions
        """
        # Discover the jobs that are in create cooloff
        jobs = self.createcooloffquery.execute()
        logging.debug("Found %s jobs in createcooloff" % len(jobs))
	processRetries(self, jobs, 'create')

         # Discover the jobs that are in submit cooloff
        jobs = self.submitcooloffquery.execute()
        logging.debug("Found %s jobs in submitcooloff" % len(jobs))
	processRetries(self, jobs, 'submit')

	# Discover the jobs that are in run cooloff
        jobs = self.jobcooloffquery.execute()
        logging.debug("Found %s jobs in jobcooloff" % len(jobs))
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
            self.deRetries()
            myThread.transaction.commit()
        except:
            myThread.transaction.rollback()
            raise
