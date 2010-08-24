#!/usr/bin/env python
"""
_ThreadPool_t_

Unit tests for WorkerThreads.

"""

__revision__ = "$Id: WorkerThreads_t.py,v 1.4 2009/02/09 12:11:45 fvlingen Exp $"
__version__ = "$Revision: 1.4 $"

import unittest
import threading
import time
import logging

from WMCore.WorkerThreads.WorkerThreadManager import WorkerThreadManager
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from Dummy import Dummy

from WMQuality.TestInit import TestInit

# local import
from Dummy import Dummy

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
    
    def setup(self, parameters):
        """
        Check the worker setup method is called
        """
        self.dummySetupCallback()
        
    def algorithm(self, parameters):
        """
        Check the algorithm method is called
        """
        self.dummyAlgoCallback()
    
    def terminate(self, parameters):
        """
        Check the terminate method is called
        """
        self.dummyTerminateCallback()

class DummyWorker2(BaseWorkerThread):
    """
    A very basic dummy worker
    """
    def algorithm(self, parameters):
        pass

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
        assert WorkerThreadsTest._setupCalled == True
        
        print('add resume workers')
        # Run the workers, pause, and check algo method gets called
        manager.resumeWorkers()
        time.sleep(3)
        manager.pauseWorkers()
        assert WorkerThreadsTest._algoCalled == True
        
        print('add terminate workers')
        # Terminate the workers, and check terminate method gets called
        manager.terminateWorkers()
        time.sleep(3)
        assert WorkerThreadsTest._terminateCalled == True
    
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
        
    def runTest(self):
        """
        Run tests
        """
        self.testA()
        self.testB()

if __name__ == "__main__":
    unittest.main()
