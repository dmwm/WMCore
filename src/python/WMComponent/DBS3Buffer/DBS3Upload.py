#!/usr/bin/env python
#pylint: disable=E1101,E1103,C0103,R0902

"""
Performs bulk DBS File(s) insertion by :
        reading the FJR received in payload
        buffering in the database
        if buffer has hit the configured limit
"""
from __future__ import print_function





import logging
import threading


from WMCore.Agent.Harness import Harness
from WMCore.WMFactory import WMFactory

from WMComponent.DBS3Buffer.DBSUploadPoller import DBSUploadPoller




class DBS3Upload(Harness):
    """
    Load the poller thread

    """

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)
        self.pollTime = 1
        print("DBS3Upload.__init__")

    def preInitialization(self):
        print("DBS3Upload.preInitialization")

        # Add event loop to worker manager
        myThread = threading.currentThread()

        pollInterval = self.config.DBS3Upload.pollInterval
        logging.info("Setting poll interval to %s seconds" % pollInterval)
        myThread.workerThreadManager.addWorker(DBSUploadPoller(self.config), pollInterval)

        return
