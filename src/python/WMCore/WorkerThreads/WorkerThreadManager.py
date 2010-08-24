#!/usr/bin/env python
"""
_WorkerThreadManager_

A class used to manage regularly running worker threads.
"""

__revision__ = "$Id: WorkerThreadManager.py,v 1.3 2009/02/01 11:41:40 jacksonj Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "james.jackson@cern.ch"

import threading
import logging

class WorkerThreadManager:
    """
    Manages regular worker slave threads
    """
    def __init__(self):
        """
        Set up the event used to terminate worker threads
        """
        self.terminateSlaves = threading.Event()
        self.pauseSlaves = threading.Event()
        self.resumeSlaves = threading.Event()
        self.lock = threading.Lock()
        self.lock.acquire()
        self.activeThreadCount = 0
        self.lock.release()
        logging.info("WorkerThreadManager: Started")
        return
    
    def slaveTerminateCallback(self):
        """
        Callback function invoked by terminated slave threads
        """
        self.lock.acquire()
        self.activeThreadCount -= 1
        self.lock.release()
    
    def prepareWorker(self, worker, frequency):
        """
        Prepares a worker thread before running
        """
        # Work timing
        worker.frequency = frequency
        
        # Thread synchronisation
        worker.terminate = self.terminateSlaves
        worker.terminateCallback = self.slaveTerminateCallback
        worker.pause = self.pauseSlaves
        worker.resume = self.resumeSlaves
        
        # Parent component
        worker.component = self.component

    def addWorker(self, worker, frequency = 60, parameters = None):
        """
        Adds a worker object and sets it running. Worker thread will sleep for
        frequency seconds between runs. Parameters, if present, are passed into
        the worker thread's setup, algorithm and terminate methods
        """
        self.PrepareWorker(worker, frequency)
        workerThread = threading.Thread(target = worker, args = (parameters,))
        msg = "WorkerThreadManager: Created worker thread %s" % str(worker)
        logging.info(msg)
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
        msg = "WorkerThreadManager: Waiting for %s worker threads to terminate"
        msg = msg % self.activeThreadCount
        logging.info(msg)
        finished = False
        while not finished:
            self.lock.acquire()
            if self.activeThreadCount == 0:
                finished = True
            self.lock.release()
            sleep(5)
        logging.info("WorkerThreadManager: All worker threads terminated")
    
    def pauseWorkers(self):
        """
        Pauses all running threads
        """
        # Don't change order without looking at BaseWorkerThread!
        self.resumeSlaves.clear()
        self.pauseSlaves.set()
    
    def resumeWorkers(self):
        """
        Resumes all running threads
        """
        # Don't change order without looking at BaseWorkerThread!
        self.pauseSlaves.clear()
        self.resumeSlaves.set()
