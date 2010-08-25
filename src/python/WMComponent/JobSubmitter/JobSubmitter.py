#!/usr/bin/env python

"""
Creates jobs for new subscriptions

"""

__revision__ = "$Id: JobSubmitter.py,v 1.2 2009/10/07 19:33:34 mnorman Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "mnorman@fnal.gov"

import logging
import threading
import time
import os.path
#import common

#WMBS objects
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.JobGroup     import JobGroup
from WMCore.WMBS.Job          import Job as WMBSJob
from WMCore.WMBS.Workflow     import Workflow
from WMCore.DAOFactory        import DAOFactory
from WMCore.WMFactory         import WMFactory
from WMCore.Agent.Harness     import Harness

from WMCore.WMSpec.WMWorkload                 import WMWorkload, WMWorkloadHelper
from WMCore.WMSpec.WMTask                     import WMTask, WMTaskHelper

from WMComponent.JobSubmitter.JobSubmitterPoller import JobSubmitterPoller


class JobSubmitter(Harness):

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)
        self.pollTime = 1

        #myThread = threading.currentThread()
        #myThread.database = os.getenv("DATABASE")
        
	logging.info("JobSubmitter.__init__")

    def preInitialization(self):
	logging.info("JobSubmitter.preInitialization")

        # use a factory to dynamically load handlers.
        factory = WMFactory('generic')
        

        # Add event loop to worker manager
        myThread = threading.currentThread()
        
        pollInterval = self.config.JobSubmitter.pollInterval
        logging.info("Setting poll interval to %s seconds" % pollInterval)
        myThread.workerThreadManager.addWorker(JobSubmitterPoller(self.config), pollInterval)

        return
