#!/usr/bin/env python
"""
_WorkerThreads_

Module to handle regular worker threads. To use, create an instance of
WorkerThreadManager to manage the worker threads. Derive worker classes from
BaseWorkerThread, and override at least the algorithm(self, parameters) method.
Run the workers by calling the WorkerThreadManager.addWorker method. Worker
threads have access to the parent thread dbFactory and component as standard.
The usual default transaction object (and transaction dictionary), trigger and
message service are attached to the child thread by the underlying framework.
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
        myThread = threading.currentThread()
        myThread.transaction.begin()
        # Do DB work here
        myThread.transaction.commit()

    def setup(self, parameters):
        print "Setting up worker with parameters:", parameters

    def terminate(self, parameters):
        print "Terminating worker with parameters:", parameters

# Create and pause a worker manager
manager = WorkerThreadManager(parentComponent)
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
