#!/usr/bin/env python
"""
Default slave, handles loading of shared DB code
"""

__all__ = []
__revision__ = \
    "$Id: DefaultSlave.py,v 1.2 2009/02/05 15:47:14 jacksonj Exp $"
__version__ = "$Revision: 1.2 $"

import threading
import pickle

from WMCore.WMFactory import WMFactory
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
                "WMComponent.WorkflowManager.Database." + myThread.dialect)
        self.queries = factory.loadObject("Queries")

    def __call__(self, parameters):
        """
        Unpickle event payload
        """
        self.messageArgs = pickle.loads(parameters['payload'])
