#!/usr/bin/env python
"""
_WorkerThreadManager_

A class used to manage regularly running worker threads.
"""

__revision__ = "$Id: WorkerThreadManager.py,v 1.8 2009/02/05 22:40:39 jacksonj Exp $"
__version__ = "$Revision: 1.8 $"
__author__ = "james.jackson@cern.ch"

import threading
import logging
import time

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

class WorkerThreadManager:
    """
    Manages regular worker slave threads
    """
    def __init__(self, component):
        """
        Set up the events used to pause, resume and terminate worker threads
        """
        self.component = component
        self.terminateSlaves = threading.Event()
        self.pauseSlaves = threading.Event()
        self.resumeSlaves = threading.Event()
        self.lock = threading.Lock()
        self.lock.acquire()
        self.activeThreadCount = 0
        self.lock.release()
        logging.info("Started")
        return
    
    def slaveTerminateCallback(self):
        """
        Callback function invoked by terminated slave threads
        """
        self.lock.acquire()
        self.activeThreadCount -= 1
        self.lock.release()
    
    def prepareWorker(self, worker, idleTime):
        """
        Prepares a worker thread before running
        """
        # Work timing
        worker.idleTime = idleTime
        worker.component = self.component
        
        # Thread synchronisation
        worker.notifyTerminate = self.terminateSlaves
        worker.terminateCallback = self.slaveTerminateCallback
        worker.notifyPause = self.pauseSlaves
        worker.notifyResume = self.resumeSlaves

    def addWorker(self, worker, idleTime = 60, parameters = None):
        """
        Adds a worker object and sets it running. Worker thread will sleep for
        idleTime seconds between runs. Parameters, if present, are passed into
        the worker thread's setup, algorithm and terminate methods
        """
        # Check type of worker
        if not isinstance(worker, BaseWorkerThread):
            msg = "Attempting to add worker that does not inherit from "
            msg += "BaseWorkerThread"
            logging.critical(msg)
            return
        
        # Prepare the new worker thread
        self.prepareWorker(worker, idleTime)
        workerThread = threading.Thread(target = worker, args = (parameters,))
        msg = "Created worker thread %s" % str(worker)
        logging.info(msg)
        
        # Increase the active thread count - note this must be done before
        # starting the thread so the callback can decrease back in case of
        # startup failure
        self.lock.acquire()
        self.activeThreadCount += 1
        self.lock.release()
        
        # Actually start the thread
        workerThread.start()

    def terminateWorkers(self):
        """
        Terminates all threads
        """
        # Don't change order without looking at BaseWorkerThread!
        # Notify all threads to terminate
        self.terminateSlaves.set()
        self.pauseSlaves.clear()
        self.resumeSlaves.set()
        
        # Wait for all threads to finished
        finished = False
        while not finished:
            self.lock.acquire()
            msg = "Waiting for %s worker threads to terminate"
            msg = msg % self.activeThreadCount
            logging.info(msg)
            if self.activeThreadCount == 0:
                finished = True
            self.lock.release()
            time.sleep(5)
        logging.info("All worker threads terminated")
    
    def pauseWorkers(self):
        """
        Pauses all running threads
        """
        # Don't change order without looking at BaseWorkerThread!
        self.resumeSlaves.clear()
        self.pauseSlaves.set()
        logging.info("All worker threads paused")
    
    def resumeWorkers(self):
        """
        Resumes all running threads
        """
        # Don't change order without looking at BaseWorkerThread!
        self.pauseSlaves.clear()
        self.resumeSlaves.set()
        logging.info("All worker threads resumed")
