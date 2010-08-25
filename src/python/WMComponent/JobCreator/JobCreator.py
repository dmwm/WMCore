#!/usr/bin/env python

"""
Creates jobs for new subscriptions
Handler implementation for polling

"""

__revision__ = "$Id: JobCreator.py,v 1.1 2009/07/09 22:12:14 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "anzar@fnal.gov"

import logging
import threading
import os

# harness class that encapsulates the basic component logic.
from WMCore.Agent.Harness import Harness
# we do not import failure handlers as they are dynamicly 
# loaded from the config file.
from WMCore.WMFactory import WMFactory

from WMComponent.JobCreator.JobCreatorPoller import JobCreatorPoller




class JobCreator(Harness):

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)
        self.pollTime = 1

        #myThread = threading.currentThread()
        #myThread.database = os.getenv("DATABASE")
        
	print "JobCreator.__init__"

    def preInitialization(self):
	print "JobCreator.preInitialization"

        # use a factory to dynamically load handlers.
        factory = WMFactory('generic')
        

        # Add event loop to worker manager
        myThread = threading.currentThread()
        
        pollInterval = self.config.JobCreator.pollInterval
        logging.info("Setting poll interval to %s seconds" % pollInterval)
        myThread.workerThreadManager.addWorker(JobCreatorPoller(self.config), pollInterval)

        return
