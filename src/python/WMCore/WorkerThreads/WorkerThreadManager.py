#!/usr/bin/env python
"""
_WorkerThreadManager_

A class used to manage regularly running worker threads.
"""
from __future__ import print_function

from builtins import object
import logging
import threading
import time

from WMCore.Agent.HeartbeatAPI import HeartbeatAPI
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

# keep track of a unique WTM number
wtmcount = 0


class WorkerThreadManager(object):
    """
    Manages regular worker slave threads
    """

    def __init__(self, component):
        """
        Set up the events used to pause, resume and terminate worker threads
        """
        global wtmcount
        self.component = component
        self.terminateSlaves = threading.Event()
        self.pauseSlaves = threading.Event()
        self.resumeSlaves = threading.Event()
        self.lock = threading.Lock()
        self.lock.acquire()
        self.activeThreadCount = 0
        self.wtmnumber = wtmcount
        wtmcount = wtmcount + 1
        self.slavecounter = 0
        self.slavelist = []
        self.lock.release()
        logging.info("Started")
        return

    def slaveTerminateCallback(self, slaveid):
        """
        Callback function invoked by terminated slave threads
        """
        self.lock.acquire()
        try:
            self.slavelist.remove("threadmanager-slave%s" % slaveid)
        except:
            pass
        else:
            self.activeThreadCount -= 1
        self.lock.release()

    def prepareWorker(self, worker, idleTime, heartbeatTimeout):
        """
        Prepares a worker thread before running
        """
        # Work timing
        worker.idleTime = idleTime
        worker.component = self.component
        self.lock.acquire()
        self.slavecounter += 1
        worker.slaveid = "%s-%s" % (self.wtmnumber, self.slavecounter)
        self.lock.release()

        # Thread synchronisation
        worker.notifyTerminate = self.terminateSlaves
        worker.terminateCallback = self.slaveTerminateCallback
        worker.notifyPause = self.pauseSlaves
        worker.notifyResume = self.resumeSlaves
        if hasattr(self.component.config, "Agent"):
            if getattr(self.component.config.Agent, "useHeartbeat", True):
                worker.heartbeatAPI = HeartbeatAPI(self.component.config.Agent.componentName,
                                                   idleTime, heartbeatTimeout)

    def addWorker(self, worker, idleTime=60, hbTimeout=None, parameters=None):
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
        self.prepareWorker(worker, idleTime, hbTimeout)
        workerThread = threading.Thread(target=worker, args=(parameters,))
        msg = "Created worker thread %s" % str(worker)
        logging.info(msg)

        # Increase the active thread count - note this must be done before
        # starting the thread so the callback can decrease back in case of
        # startup failure
        self.lock.acquire()
        self.activeThreadCount += 1
        workerThread.name = "threadmanager-slave%s" % worker.slaveid
        self.slavelist.append(workerThread.name)
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
            logging.info("Waiting for %s worker threads to terminate", self.activeThreadCount)
            logging.debug("\n slavelist is %s", self.slavelist)
            logging.debug("\n threadlist is %s", threading.enumerate())

            if self.activeThreadCount == 0:
                finished = True
            else:
                # check to make sure we aren't waiting on dead threads
                threadlist = threading.enumerate()
                # we want to look for all the slavenames
                #  that correspond to nonexistant or dead threads
                # also, I realize this is O(N^2), but my brain hurts too hard
                #  to do it nicer

                # this (should be) race-proof. my thoughts:
                #  this is in a lock, the only other places that modify
                #  activeThreadCount are within locks
                # everywhere else that tries to modify active thread count
                # also does it with a try-except-else around removing from
                # slavelist. Slavelist becomes our synchronization
                for slavename in self.slavelist:
                    found = False
                    for threadobj in threadlist:
                        if hasattr(threadobj, 'name') and (slavename == threadobj.name) and (threadobj.is_alive()):
                            found = True
                    if found is False:
                        # the slave we wanted wasn't running
                        try:
                            self.slavelist.remove(slavename)
                        except Exception as ex:
                            print("couldn't remove thread.. %s " % ex)
                        else:
                            self.activeThreadCount -= 1
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
