#!/usr/bin/env python
"""
_JobAccountant_

JobAccountant harness.  Instantiate the JobAccountandPoller and have it poll
WMBS for complete jobs.
"""




import threading

from WMCore.Agent.Harness import Harness
from WMComponent.JobAccountant.JobAccountantPoller import JobAccountantPoller

class JobAccountant(Harness):
    def __init__(self, config):
        Harness.__init__(self, config)
        return

    def preInitialization(self):
        pollInterval = self.config.JobAccountant.pollInterval
        myThread = threading.currentThread()
        myThread.workerThreadManager.addWorker(JobAccountantPoller(self.config), pollInterval)
