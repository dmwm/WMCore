#!/usr/bin/env python
"""
Default slave for FeederManager
"""

__all__ = []
__revision__ = \
    "$Id: DefaultSlave.py,v 1.2 2009/11/06 11:26:45 riahi Exp $"
__version__ = "$Revision: 1.2 $"

import threading
import pickle

from WMCore.ThreadPool.ThreadSlave import ThreadSlave
from WMCore.WMFactory import WMFactory

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
        self.queries = None


    def initInThread(self):
        """
        Load shared queries
        """
        # Call parent initialisation
        ThreadSlave.initInThread(self)
        
        # Load backend queries
        myThread = threading.currentThread()
        factory = WMFactory("default", \
            "WMComponent.FeederManager.Database." + myThread.dialect)
        self.queries = factory.loadObject("Queries")

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


