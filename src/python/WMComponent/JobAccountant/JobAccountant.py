#!/usr/bin/env python
"""
_JobAccountant_

JobAccountant harness.  Instantiate the JobAccountandPoller and have it poll
WMBS for complete jobs.
"""

__revision__ = "$Id: JobAccountant.py,v 1.6 2009/11/10 15:31:59 sfoulkes Exp $"
__version__ = "$Revision: 1.6 $"

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
