#!/usr/bin/env python
"""
_ThreadPool_t_

Unit tests for threadpool.

"""

__revision__ = "$Id: ThreadPool_t.py,v 1.2 2008/09/04 14:32:08 fvlingen Exp $"
__version__ = "$Revision: 1.2 $"

import commands
import unittest
import logging
import os
import threading
import time

from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction
from WMCore.WMFactory import WMFactory
from WMCore.ThreadPool.ThreadPool import ThreadPool

# local import
from Dummy import Dummy

class ThreadPoolTest(unittest.TestCase):
    """
    _ThreadPool_t_
    
    Unit tests for threadpool
    
    """

    _setup = False
    _teardown = False
    _nrOfThreads = 100
    _nrOfPools = 5 

    def setUp(self):
        "make a logger instance and create tables"
       
        if not ThreadPoolTest._setup: 
            logging.basicConfig(level=logging.NOTSET,
                format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                datefmt='%m-%d %H:%M',
                filename='%s.log' % __file__,
                filemode='w')

            myThread = threading.currentThread()
            myThread.logger = logging.getLogger('ThreadPoolTest')
            myThread.dialect = 'MySQL'
        
            options = {}
            options['unix_socket'] = os.getenv("DBSOCK")
            myThread.dbFactory = DBFactory(myThread.logger, \
                os.getenv("MYSQLDATABASE"), options)
            myThread.dbi = myThread.dbFactory.connect() 
            myThread.transaction = Transaction(myThread.dbi)


            factory = WMFactory("msgService", "WMCore.ThreadPool."+ \
                myThread.dialect)
            create = factory.loadObject("Create")
            createworked = create.execute(conn = myThread.transaction.conn)
            if createworked:
                logging.debug("ThreadPool tables created")
            else:
                logging.debug("ThreadPool tables could not be created, \
                    already exists?")
            myThread.transaction.commit()                                  
            ThreadPoolTest._setup = True

    def tearDown(self):
        """
        Delete the databases
        """
        myThread = threading.currentThread()
        if ThreadPoolTest._teardown:
            myThread.logger.debug(commands.getstatusoutput('echo yes | mysqladmin -u root --socket='+os.getenv("DBSOCK")+' drop '+os.getenv("DBNAME")))
            myThread.logger.debug(commands.getstatusoutput('mysqladmin -u root --socket='+os.getenv("DBSOCK")+' create '+os.getenv("DBNAME")))
            myThread.logger.debug("database deleted")
               
               
    def testA(self):
        """
        __testSubscribe__

        Test subscription of a component.
        """
        myThread = threading.currentThread()
        # create a 'fake' component that contains a arg dictionary.
        component = Dummy()
        args = {}
        args['db_dialect'] = 'mysql'
        args['db_socket'] = os.getenv("DBSOCK")
        args['db_user'] = os.getenv("DBUSER")
        args['db_pass'] = os.getenv("DBPASS")
        args['db_hostname'] = os.getenv("DBHOST")
        args['db_name'] = os.getenv("DBNAME")
        args['componentName'] = 'TestComponent'
        component.args = args

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
        
if __name__ == "__main__":
    unittest.main()
