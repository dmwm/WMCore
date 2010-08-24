#!/usr/bin/env python
#pylint: disable-msg=E1101,C0103,R0902
"""
_ThreadPool_t_

Unit tests for threadpool.

"""

__revision__ = "$Id: ThreadPool_t.py,v 1.6 2008/12/18 14:53:17 fvlingen Exp $"
__version__ = "$Revision: 1.6 $"

import unittest
import threading
import time

from WMCore.ThreadPool.ThreadPool import ThreadPool

from WMQuality.TestInit import TestInit

# local import
from Dummy import Dummy

class ThreadPoolTest(unittest.TestCase):
    """
    _ThreadPool_t_
    
    Unit tests for threadpool
    
    """

    _setup = False
    _teardown = False
    _nrOfThreads = 10
    _nrOfPools = 5

    def setUp(self):
        "make a logger instance and create tables"
       
        if not ThreadPoolTest._setup: 
            # initialization necessary for proper style.
            myThread = threading.currentThread()
            myThread.dialect = None
            myThread.transaction = None

            self.testInit = TestInit(__file__)
            self.testInit.setLogging()
            self.testInit.setDatabaseConnection()
            self.testInit.setSchema()

            ThreadPoolTest._setup = True

    def tearDown(self):
        """
        Deletion of database
        """
        # FIXME: this might not work if your not using socket.
        myThread = threading.currentThread()
        if ThreadPoolTest._teardown and myThread.dialect == 'MySQL':
            # call the script we use for cleaning:
            self.testInit.clearDatabase()
        ThreadPoolTest._teardown = False
               
    def testA(self):
        """
        __testSubscribe__

        Test subscription of a component.
        """
        ThreadPoolTest._teardown = True
        myThread = threading.currentThread()
        # create a 'fake' component that contains a arg dictionary.
        component = Dummy()
        # load default parameters.
        config = self.testInit.getConfiguration()
        # normally assigned by the harness of the test component.
        config.Agent.componentName = "TestComponent"

        component.config = config

        threadPools = []
        for i in xrange(0, ThreadPoolTest._nrOfPools):
            threadPool = ThreadPool("WMCore.ThreadPool.ThreadSlave", \
                component, 'MyPool_'+str(i), ThreadPoolTest._nrOfThreads)
            threadPools.append(threadPool)
        # this is how you would use the threadpool. The threadpool retrieves
        # events/payloads from the message service. If a thread is available
        # it is dispatched, otherwise it is stored in the trheadpool.
        # make the number of tasks bigger than number of threads to tesT
        # the persistent queue.
        for i in xrange(0, ThreadPoolTest._nrOfThreads*10):
            event = 'eventNr_'+str(i)
            payload = 'payloadNr_'+str(i)
            # normally you would have different events per threadpool and 
            # even different objects per pool. the payload part will be 
            # pickled into the database enabling flexibility in passing 
            # information.
            for j in xrange(0, ThreadPoolTest._nrOfPools):
                threadPools[j].enqueue(event, \
                    {'event' : event, 'payload' : payload})
        # this commit you want to be in the agent harness, so the message is
        # actual removed from the msgService. we can do this as the threadpool
        # acts as a dispatcher and is a shortlived action: dispatch to thread
        # or queu and tell agent harness it is finished.
        finished = False
        while not finished: 
            print('waiting for threads to finishs. Work left:')
            for j in xrange(0, ThreadPoolTest._nrOfPools):
                print('pool_'+str(j)+ ':' + str(threadPools[j].callQueue))
            time.sleep(1)
            finished = True
            for j in xrange(0, ThreadPoolTest._nrOfPools):
                if (len(threadPools[j].resultsQueue) < ThreadPoolTest._nrOfThreads*10):
                    finished = False
                    break
        # check if the tables are really empty and all messages 
        # have been processed.
        for j in xrange(0, ThreadPoolTest._nrOfPools):
            assert len(threadPools[j].resultsQueue) == \
                ThreadPoolTest._nrOfThreads*10
        myThread.transaction.begin()
        for j in xrange(0, ThreadPoolTest._nrOfPools):
            assert threadPools[j].countMessages() == 0
        myThread.transaction.commit()
  
    def runTest(self):
        """ 
        Run the threadpool tests.
        """
        self.testA()

if __name__ == "__main__":
    unittest.main()
