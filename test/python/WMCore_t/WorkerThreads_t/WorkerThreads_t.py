#!/usr/bin/env python
"""
_ThreadPool_t_

Unit tests for WorkerThreads.

"""
from __future__ import absolute_import
from __future__ import print_function

import logging
import threading
import time
import unittest

from Utils.Timers import timeFunction
# local import
from WMCore_t.WorkerThreads_t.Dummy import Dummy

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.WorkerThreads.WorkerThreadManager import WorkerThreadManager
from WMQuality.TestInit import TestInit


class DummyWorker1(BaseWorkerThread):
    """
    Dummy class to implement a minimal worker object, used to check that all
    required worker methods get called
    """

    def __init__(self):
        """
        Get a callback to the test object used to access "thread doing stuff"
        data
        """
        BaseWorkerThread.__init__(self)
        myThread = threading.currentThread()
        self.dummySetupCallback = myThread.dummySetupCallback
        self.dummyAlgoCallback = myThread.dummyAlgoCallback
        self.dummyTerminateCallback = myThread.dummyTerminateCallback
        if 'workerThreadManager' in dir(myThread):
            self.workerThreadManager = myThread.workerThreadManager

    def setup(self, parameters):
        """
        Check the worker setup method is called
        """
        logging.info("DummyWorker1 setup called")
        self.dummySetupCallback()

    @timeFunction
    def algorithm(self, parameters):
        """
        Check the algorithm method is called
        """
        logging.info("DummyWorker1 algorithm called")
        self.dummyAlgoCallback()

    def terminate(self, parameters):
        """
        Check the terminate method is called
        """
        logging.info("DummyWorker1 terminate called")
        self.dummyTerminateCallback()
        # FIXME why this call doesn't reach the parent class?!?!
        super(DummyWorker1, self).terminate(parameters)


class DummyWorker2(BaseWorkerThread):
    """
    A very basic dummy worker
    """

    @timeFunction
    def algorithm(self, parameters):
        pass


class ErrorWorker(DummyWorker1):
    """
    A worker that throws an error
    """

    @timeFunction
    def algorithm(self, parameters):
        # workerThreadManager will be added by Harness
        # that isnt used here so add manually
        myThread = threading.currentThread()
        myThread.workerThreadManager = self.workerThreadManager
        raise RuntimeError("ErrorWorker throws errors")


class WorkerThreadsTest(unittest.TestCase):
    """
    Unit tests for WorkerThreads
    """

    _setupCalled = False
    _algoCalled = False
    _terminateCalled = False

    def dummySetupCallback(self):
        """
        Callback for setup
        """
        WorkerThreadsTest._setupCalled = True

    def dummyAlgoCallback(self):
        """
        Callback for algo
        """
        WorkerThreadsTest._algoCalled = True

    def dummyTerminateCallback(self):
        """
        Callback for terminate
        """
        WorkerThreadsTest._terminateCalled = True

    def setUp(self):
        "make a logger instance and create tables"

        # initialization necessary for proper style.
        myThread = threading.currentThread()
        myThread.dialect = None
        myThread.transaction = None
        myThread.dbFactory = None

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()

        # Attack check callbacks to current thread
        myThread = threading.currentThread()
        myThread.dummySetupCallback = self.dummySetupCallback
        myThread.dummyAlgoCallback = self.dummyAlgoCallback
        myThread.dummyTerminateCallback = self.dummyTerminateCallback

    def tearDown(self):
        """
        Deletion of database
        """
        # FIXME: this might not work if your not using socket.
        myThread = threading.currentThread()

        # Remove callback methods from thread
        myThread = threading.currentThread()
        del myThread.dummySetupCallback
        del myThread.dummyAlgoCallback
        del myThread.dummyTerminateCallback

    def testA(self):
        """
        Check worker methods get called. We sleep occasionally to ensure these
        asynchronous calls have enough time to be called
        """
        # Create a worker manager
        compDummy = Dummy()
        print('create manager')
        manager = WorkerThreadManager(compDummy)

        # Pause it
        print('pause workers')
        manager.pauseWorkers()

        # Add a worker, and check init method gets called
        print('add worker')
        manager.addWorker(DummyWorker1(), 1)
        time.sleep(3)
        self.assertEqual(WorkerThreadsTest._setupCalled, True)
        # Ensure the algo wasn't called whilst paused
        self.assertEqual(WorkerThreadsTest._algoCalled, False)

        print('resume workers')
        # Run the workers, pause, and check algo method gets called
        manager.resumeWorkers()
        time.sleep(3)
        manager.pauseWorkers()
        self.assertEqual(WorkerThreadsTest._algoCalled, True)

        print('terminate workers')
        # Terminate the workers, and check terminate method gets called
        manager.terminateWorkers()
        time.sleep(3)
        self.assertEqual(WorkerThreadsTest._terminateCalled, True)

    def testB(self):
        """
        Check we can terminate before pausing workers
        """
        compDummy = Dummy()
        print('create manager')
        manager = WorkerThreadManager(compDummy)
        print('add worker')
        manager.addWorker(DummyWorker2(), 1)
        print('terminate worker')
        manager.terminateWorkers()

    def testWorkerError(self):
        """If a worker raises an exception terminate the entire component"""
        compDummy = Dummy()

        print('create manager')
        manager = WorkerThreadManager(compDummy)
        # needed for handling errors - harness would generally set this
        myThread = threading.currentThread()
        myThread.workerThreadManager = manager

        print('add workers')
        manager.addWorker(DummyWorker1(), 0.1)
        manager.addWorker(ErrorWorker(), 0.1)

        print('run workers, one will throw an error')
        manager.resumeWorkers()

        # should do something smarter here
        # too short a time and threads havent exited yet
        time.sleep(6)

        # all threads should have ended after worker raised exception
        self.assertEqual(manager.activeThreadCount, 0)


if __name__ == "__main__":
    unittest.main()
