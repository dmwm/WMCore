#!/usr/bin/env python

"""
Auto generated stub be careful with editing,
Inheritance is preferred.
"""
import threading
import logging
# harness class that encapsulates the basic component logic.
from WMCore.Agent.Harness import Harness
from WMAgentTasks import WMAgentTasks
from WMQuality.Emulators.DataBlockGenerator import Globals
class WMAgentEmulator(Harness):


    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)
        

    def preInitialization(self):
        resources = self.populateResources()
        # Add event loop to worker manager
        myThread = threading.currentThread()
        pollInterval = 1
        logging.info("Setting poll interval to %s seconds" % pollInterval)
        myThread.workerThreadManager.addWorker(WMAgentTasks(resources), pollInterval)
        
        return
                                           
    def populateResources(self):
        """
        emulating resource db which can represent
        {site: job} format
        """
        jobSites = {}
        for site in Globals.SITES:
            jobSites[site] = 100
        return jobSites
    