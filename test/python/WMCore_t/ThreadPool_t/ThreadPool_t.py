#!/usr/bin/env python
#pylint: disable-msg=E1101,C0103,R0902
"""
_ThreadPool_t_

Unit tests for threadpool.

"""

__revision__ = "$Id: ThreadPool_t.py,v 1.9 2009/10/13 22:42:58 meloam Exp $"
__version__ = "$Revision: 1.9 $"

import unittest
import threading
import time
import os

from WMCore.ThreadPool.ThreadPool import ThreadPool
from WMCore.WMFactory             import WMFactory

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
            myThread.dialect = os.getenv("DIALECT")
            myThread.transaction = None

            self.testInit = TestInit(__file__, os.getenv("DIALECT"))
            self.testInit.setLogging()
            self.testInit.setDatabaseConnection()
            #self.tearDown()
            self.testInit.setSchema()


            ThreadPoolTest._setup = True

    def tearDown(self):
        """
        Deletion of database
        """
        # FIXME: this might not work if your not using socket.
        myThread = threading.currentThread()

        factory = WMFactory("WMBS", "WMCore.ThreadPool")
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        myThread.transaction.begin()
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete ThreadPool tear down.")
        myThread.transaction.commit()
        
        
        factory = WMFactory("WMBS", "WMCore.MsgService")
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        myThread.transaction.begin()
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete MsgService tear down.")
        myThread.transaction.commit()
        
        factory = WMFactory("Trigger", "WMCore.Trigger")
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        myThread.transaction.begin()
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete Trigger tear down.")
        myThread.transaction.commit()

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
        config.CoreDatabase.dialect = os.getenv("DIALECT")

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
        # or queue and tell agent harness it is finished.
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
  

if __name__ == "__main__":
    unittest.main()
