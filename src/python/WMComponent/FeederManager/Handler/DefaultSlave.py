#!/usr/bin/env python
"""
Default slave for FeederManager
"""

__all__ = []
__revision__ = \
    "$Id: DefaultSlave.py,v 1.1 2009/02/02 23:06:49 jacksonj Exp $"
__version__ = "$Revision: 1.1 $"

import logging
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

    def initInThread(self):
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
