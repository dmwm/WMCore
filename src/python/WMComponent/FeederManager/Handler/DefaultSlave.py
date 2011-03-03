#!/usr/bin/env python
"""
Default slave for FeederManager
"""
__all__ = []

import threading
import pickle

from WMCore.ThreadPool.ThreadSlave import ThreadSlave
from WMCore.DAOFactory import DAOFactory

class DefaultSlave(ThreadSlave):
    """
    The default slave for FeederManager messages
    """

    def __init__(self):
        """
        Initialise the slave
        """
        ThreadSlave.__init__(self)
        myThread = threading.currentThread()
        self.runningFeedersLock = myThread.runningFeedersLock
        self.runningFeeders = myThread.runningFeeders
        self.messageArgs = None

    def initInThread(self):
        """
        Load shared queries
        """
        # Call parent initialisation
        ThreadSlave.initInThread(self)

        myThread = threading.currentThread()

        daofactory = DAOFactory(package = "WMComponent.FeederManager.Database" , \
              logger = myThread.logger, \
              dbinterface = myThread.dbi)

        # Load queries objects
        self.checkFeeder = daofactory(classname = "CheckFeeder")
        self.getFeederId = daofactory(classname = "GetFeederId")
        self.addFeeder = daofactory(classname = "AddFeeder")
        self.addFilesetToManage = daofactory(classname = "AddFilesetToManage")

        # Get feeder objects
        myThread.runningFeedersLock = self.runningFeedersLock
        myThread.runningFeeders = self.runningFeeders

    def __call__(self, parameters):
        """
        Unpickle event payload if it is pickled
        """
        try:
            self.messageArgs = pickle.loads(parameters['payload'])
        except:
            self.messageArgs = parameters['payload']
