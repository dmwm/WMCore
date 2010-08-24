#!/usr/bin/env python
"""
Default slave, handles loading of shared DB code
"""

__all__ = []
__revision__ = \
    "$Id: DefaultSlave.py,v 1.1 2009/02/05 14:45:02 jacksonj Exp $"
__version__ = "$Revision: 1.1 $"

import threading
import pickle

from WMCore.ThreadPool.ThreadSlave import ThreadSlave

class DefaultSlave(ThreadSlave):
    """
    The default slave for a WorkerManager handles
    """
    def initInThread(self):
        # Call superclass setup
        ThreadSlave.initInThread(self)
        
        # Load DB queries
        myThread = threading.currentThread()
        factory = WMFactory("default", \
                "WMComponent.WorkerManager.Database." + myThread.dialect)
        self.queries = factory.loadObject("Queries")

    def __call__(self, parameters):
        """
        Unpickle event payload
        """
        self.messageArgs = pickle.loads(parameters['payload'])
