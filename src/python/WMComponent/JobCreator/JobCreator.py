#!/usr/bin/env python
#pylint: disable-msg=W0613, W6501

"""
Creates jobs for new subscriptions
Handler implementation for polling

"""




import logging
import threading

# harness class that encapsulates the basic component logic.
from WMCore.Agent.Harness import Harness

from WMComponent.JobCreator.JobCreatorPoller import JobCreatorPoller




class JobCreator(Harness):
    """
    Creates jobs for new subscriptions
    Handler implementation for polling
    
    """

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)
        self.pollTime = 1

        #myThread = threading.currentThread()
        #myThread.database = os.getenv("DATABASE")
        
        print "JobCreator.__init__"

    def preInitialization(self):
        """
        Step that actually adds the worker thread properly

        """
        print "JobCreator.preInitialization"


        # Add event loop to worker manager
        myThread = threading.currentThread()
        
        pollInterval = self.config.JobCreator.pollInterval
        logging.info("Setting poll interval to %s seconds" % pollInterval)
        myThread.workerThreadManager.addWorker(JobCreatorPoller(self.config), \
                                               pollInterval)

        return
