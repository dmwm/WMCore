#!/usr/bin/env python
"""
_WorkerThreads_

Module to handle regular worker threads. To use, create an instance of
WorkerThreadManager to manage the worker threads. Derive worker classes from
BaseWorkerThread, and override at least the algorithm(self, parameters) method.
Run the workers by calling the WorkerThreadManager.addWorker method. Worker
threads have access to the parent thread dbFactory and component as standard.
E.g.:

from WMCore.WorkerThreads.WorkerThreadManager import WorkerThreadManager
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
import time, threading

# Create a new worker object
class MyWorker(BaseWorkerThread):
    def __init__(self):
        BaseWorkerThread.__init__(self)
    def algorithm(self, parameters):
        print "Doing work with parameters:", parameters
    def setup(self, parameters):
        print "Setting up worker with parameters:", parameters
    def terminate(self, parameters):
        print "Terminating worker with parameters:", parameters

# Fake a DB Factory
threading.currentThread().dbFactory = None

# Create and pause a worker manager
manager = WorkerThreadManager(None)
manager.pauseWorkers()

# Add a MyWorker() instance to run every 2 seconds and one every 3 seconds
manager.addWorker(MyWorker(), 2, ["Some", "Parameters"])
manager.addWorker(MyWorker(), 3, ["Some again", "Parameters again"])

# Run the worker threads
manager.resumeWorkers()
time.sleep(5)

# Terminate all worker threads
manager.terminateWorkers()

"""

__revision__ = "$Id: __init__.py,v 1.3 2009/02/01 17:52:46 jacksonj Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "james.jackson@cern.ch"