#!/usr/bin/env python
"""
_WorkerThreadManager_

A class used to manage regularly running worker threads.
"""

__revision__ = "$Id: WorkerThreadManager.py,v 1.2 2009/02/01 11:04:28 jacksonj Exp $"
__version__ = "$Revision: 1.2 $"
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
        logging.info("Started WorkerThreadManager")
        return
    
    def prepareWorker(self, worker, frequency):
        """
        Prepares a worker thread before running
        """
        worker.frequency = frequency
        worker.terminate = self.terminateSlaves
        worker.pause = self.pauseSlaves
        worker.resume = self.resumeSlaves
        worker.component = self.component

    def addWorker(self, worker, frequency = 60, parameters = None):
        """
        Adds a worker object and sets it running. Worker thread will sleep for
        frequency seconds between runs. Parameters, if present, are passed into
        the worker thread's setup, algorithm and terminate methods
        """
        self.PrepareWorker(worker, frequency)
        workerThread = threading.Thread(target = worker, args = (parameters,))
        workerThread.start()

    def terminateWorkers(self):
        """
        Terminates all threads
        """
        # Don't change order without looking at BaseWorkerThread!
        self.terminateSlaves.set()
        self.pauseSlaves.clear()
        self.resumeSlaves.set()
    
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
