#!/usr/bin/env python


"""
Checks for finished subscriptions
Upon finding finished subscriptions, notifies WorkQueue and kills them
"""
from __future__ import print_function




import logging
import threading

from WMCore.Agent.Harness import Harness
from WMCore.WMFactory import WMFactory

from WMComponent.TaskArchiver.TaskArchiverPoller import TaskArchiverPoller
from WMComponent.TaskArchiver.CleanCouchPoller import CleanCouchPoller

class TaskArchiver(Harness):

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)
        self.pollTime = 1

        self.config = config

        print("TaskArchiver.__init__")

    def preInitialization(self):
        print("TaskArchiver.preInitialization")

        # Add event loop to worker manager
        myThread = threading.currentThread()

        pollInterval = self.config.TaskArchiver.pollInterval
        logging.info("Setting task archiver poll interval to %s seconds" % pollInterval)
        myThread.workerThreadManager.addWorker(TaskArchiverPoller(self.config), pollInterval)

        couchInterval = self.config.TaskArchiver.cleanCouchInterval
        logging.info("Setting poll interval for cleanup old couch doc to %s seconds" % couchInterval)
        myThread.workerThreadManager.addWorker(CleanCouchPoller(self.config),
                                               couchInterval)

        return
