#!/usr/bin/env python
"""
Default slave, handles loading of shared DB code
"""

__all__ = []
__revision__ = \
    "$Id: DefaultSlave.py,v 1.3 2009/02/05 23:21:43 jacksonj Exp $"
__version__ = "$Revision: 1.3 $"

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
        Unpickle event payload
        """
        self.messageArgs = pickle.loads(parameters['payload'])
