#!/usr/bin/env python


"""
Checks for files in state Complete and handles them
"""

__revision__ = "$Id: JobAccountant.py,v 1.1 2009/07/28 21:41:18 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "mnorman@fnal.gov"

import logging
import threading

# harness class that encapsulates the basic component logic.
from WMCore.Agent.Harness import Harness
# we do not import failure handlers as they are dynamicly 
# loaded from the config file.
from WMCore.WMFactory import WMFactory

from WMComponent.JobAccountant.JobAccountantPoller import JobAccountantPoller

from WMComponent.DBSBuffer.DBSBuffer import DBSBuffer




class JobAccountant(Harness):

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)
        self.pollTime = 1

        self.config = config
        
	print "JobAccountant.__init__"

    def preInitialization(self):
	print "JobAccountant.preInitialization"

        # Add event loop to worker manager
        myThread = threading.currentThread()

        print "About to run test"
        testDBSBuffer = DBSBuffer(self.config)

        print "2a"
        
        testDBSBuffer.prepareToStart()
        
        pollInterval = self.config.JobAccountant.pollInterval
        logging.info("Setting poll interval to %s seconds" % pollInterval)
        myThread.workerThreadManager.addWorker(JobAccountantPoller(self.config, testDBSBuffer), pollInterval)

        return
