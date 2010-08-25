#!/usr/bin/env python
"""
Default slave, handles loading of shared DB code
"""

__all__ = []


import threading
import pickle

from WMCore.WMFactory import WMFactory
from WMCore.ThreadPool.ThreadSlave import ThreadSlave

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
        self.queries = None

    def initInThread(self):
        """
        Load shared queries
        """
        # Call superclass setup
        ThreadSlave.initInThread(self)
        
        # Load DB queries
        myThread = threading.currentThread()
        factory = WMFactory("default", \
                "WMComponent.WorkflowManager.Database." + myThread.dialect)
        self.queries = factory.loadObject("Queries")

    def __call__(self, parameters):
        """
        Unpickle event payload if it is pickled
        """
        try:
            self.messageArgs = pickle.loads(parameters['payload'])
        except:
            self.messageArgs = parameters['payload']
