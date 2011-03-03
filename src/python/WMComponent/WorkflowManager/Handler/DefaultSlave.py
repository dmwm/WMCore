#!/usr/bin/env python
"""
Default slave, handles loading of shared DB code
"""

__all__ = []

import threading
import pickle

from WMCore.ThreadPool.ThreadSlave import ThreadSlave
from WMCore.DAOFactory import DAOFactory

class DefaultSlave(ThreadSlave):
    """
    Base class for all WorkflowManager slave handlers
    """
    def __init__(self):
        """
        Setup the slave data members
        """
        ThreadSlave.__init__(self)
        self.messageArgs = None

    def initInThread(self):
        """
        Load shared queries
        """
        # Call superclass setup
        ThreadSlave.initInThread(self)

        # Load DB queries
        myThread = threading.currentThread()
        daofactory = DAOFactory(package = "WMComponent.WorkflowManager.Database" , \
              logger = myThread.logger, \
              dbinterface = myThread.dbi)

        self.addManagedWorkflow = daofactory(classname = "AddManagedWorkflow")
        self.markLocation = daofactory(classname = "MarkLocation")
        self.unmarkLocation = daofactory(classname = "UnmarkLocation")
        self.removeManagedWorkflow = daofactory(classname = "RemoveManagedWorkflow")

    def __call__(self, parameters):
        """
        Unpickle event payload if it is pickled
        """
        try:
            self.messageArgs = pickle.loads(parameters['payload'])
        except:
            self.messageArgs = parameters['payload']
