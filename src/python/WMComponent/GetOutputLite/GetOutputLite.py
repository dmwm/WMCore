#!/usr/bin/env python


"""
Get all batch/middleware done jobs and retrieve those
Perform some post mortem operation
"""


import logging
import threading

from WMCore.Agent.Harness import Harness
#from WMCore.WMFactory import WMFactory

from WMComponent.GetOutputLite.GetOutputPoller import GetOutputPoller


class GetOutputLite(Harness):
    """
    Checks for finished subscriptions
    Upon finding finished subscriptions, notifies WorkQueue and kills them

    """

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)

        self.config = config
        
        print "GetOutputLite.__init__"

    def preInitialization(self):
        """
        Sets up the worker thread

        """
        logging.info("GetOutputLite.preInitialization")

        # Add event loop to worker manager
        myThread = threading.currentThread()
        logging.info(str(myThread))

        logging.info("Setting poll interval to %s seconds" \
                      %str(self.config.GetOutputLite.pollInterval) )
        myThread.workerThreadManager.addWorker( \
                              GetOutputPoller(self.config), \
                              self.config.GetOutputLite.pollInterval \
                            )

        return
