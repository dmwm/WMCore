#!/usr/bin/env python
"""
_JobAccountant_

Poll WMBS for complete jobs and process their framework job reports.
"""

__revision__ = "$Id: JobAccountant.py,v 1.2 2009/10/14 16:49:02 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

import time
import threading

from WMCore.Agent.Harness import Harness
from WMCore.DAOFactory import DAOFactory
from WMCore.ProcessPool.ProcessPool import ProcessPool

class JobAccountant(Harness):
    def __init__(self, config):
        Harness.__init__(self, config)
        self.config = config
        self.pollInterval = config.JobAccountant.pollInterval
        return
    
    def preInitialization(self):
        """
        _preInitialization_

        Instantiate the requisite number of accountant workers and create a
        processpool with them.  Also instantiate all the DAOs that we will use.
        """
        self.processPool = ProcessPool("JobAccountant.AccountantWorker",
                                       totalSlaves = self.config.JobAccountant.workerThreads,
                                       componentDir = self.config.JobAccountant.componentDir,
                                       config = self.config)

        myThread = threading.currentThread()
        daoFactory = DAOFactory(package = "WMCore.WMBS", logger = myThread.logger,
                                dbinterface = myThread.dbi)
        self.getJobsAction = daoFactory(classname = "Jobs.GetFWJRByState")
        return
    
    def pollForJobs(self):
        """
        _pollForJobs_

        Poll WMBS for jobs in the "Complete" state and then pass them to the
        ThreadPool so that they can be processed.
        """
        completeJobs = self.getJobsAction.execute(state = "complete")
        self.processPool.enqueue(completeJobs)
        return

    def startComponent(self):
        """
        _startComponent_

        Start the component.  Loop forever and poll for jobs.
        """
        myThread = threading.currentThread()
        
        while True:
            self.pollForJobs()
            time.sleep(self.pollInterval)
            
        return
