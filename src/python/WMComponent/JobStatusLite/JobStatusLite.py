#!/usr/bin/env python


"""
Checks for finished subscriptions
Upon finding finished subscriptions, notifies WorkQueue and kills them
"""
from __future__ import print_function




import logging
import threading

from WMCore.Agent.Harness import Harness
#from WMCore.WMFactory import WMFactory

from WMCore.BossAir.StatusPoller import StatusPoller


class JobStatusLite(Harness):
    """
    Checks for finished subscriptions
    Upon finding finished subscriptions, notifies WorkQueue and kills them

    """

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)

        self.config = config
        #self.config.JobStatusLite.pollInterval
        #self.config.JobStatusLite.queryInterval

        print("JobStatusLite.__init__")

    def preInitialization(self):
        """
        Sets up the worker thread

        """
        logging.info("JobStatusLite.preInitialization")

        # Add event loop to worker manager
        myThread = threading.currentThread()
        logging.info(str(myThread))


        logging.info("Setting poll interval to %s seconds" \
                      %str(self.config.JobStatusLite.pollInterval) )
        myThread.workerThreadManager.addWorker( \
                              StatusPoller(self.config), \
                              self.config.JobStatusLite.pollInterval \
                            )

        return
