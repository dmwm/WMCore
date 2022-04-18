#!/usr/bin/env python
"""
_ThreadPool_

A class used for createing thread pools.
To use this you need to use the ThreadSlave class
"""






from builtins import str

import base64
import logging
import random
import threading
import time
from Utils.Utilities import encodeUnicodeToBytes
import pickle

from WMCore.ThreadPool.WorkQueue import ThreadPool as Queue
from WMCore.WMFactory import WMFactory

from Utils.PythonVersion import PY3

class ThreadPool(Queue):
    """
    _ThreadPool_

    A class used for creating persistent thread pools.
    To use this you need to use the ThreadSlave class
    """

    def __init__(self, slaveModule, component, \
        threadPoolID= 'default', nrOfSlaves = 0):
        """
        _init_

        Initializes pool, and resets lost threads (e.g. lost during crash).

        """

        #Queue.__init__(self, slaves)
        Queue.__init__(self, [])
        self.component = component
        self.callQueue = 0
        # we augment the threadpool id with component name in case
        # we use separate talbes for the thread queues to prevent
        # name clashes.
        compName = self.component.config.Agent.componentName
        self.threadPoolId = 'tp_threadpool_'+ \
            compName+'_'+threadPoolID
        # if set to false the peristent thread pool table is
        # created per threadpool id.
        self.oneQueue = True
        # the size of the buffer to minimize single inserts in large tables.
        self.bufferSize = 2
        # the maximum number of slaves (threads) we allow.
        self.nrOfSlaves = nrOfSlaves

        self.slaveName = slaveModule.split(".")[-1]

        self.slaveFactory = WMFactory("slaveFactory", \
            slaveModule[0:slaveModule.rfind(slaveModule.split(".")[-1])-1])
        myThread = threading.currentThread()
        myThread.transaction.begin()
        factory = WMFactory("threadPool", "WMCore.ThreadPool."+ \
            myThread.dialect)
        self.query = factory.loadObject("Queries")

        # check which tables we need to use.
        self.poolTable = 'tp_threadpool'
        self.poolTableBufferIn = 'tp_threadpool_buffer_in'
        self.poolTableBufferOut = 'tp_threadpool_buffer_out'
        if not self.oneQueue:
            self.poolTable = self.threadPoolId
            self.poolTableBufferIn = 'tp_threadpool_'+\
                compName+'_'+\
                threadPoolID+'_buffer_in'
            self.poolTableBufferOut = 'tp_threadpool_'+\
                compName+'_'+\
                threadPoolID+'_buffer_out'
            self.query.insertThreadPoolTables(self.poolTable)
            self.query.insertThreadPoolTables(self.poolTableBufferIn)
            self.query.insertThreadPoolTables(self.poolTableBufferOut)

        # restore any threads that might have been lost during a crash
        # de register thread in database so we do not need to restore it.
        msg = "THREADPOOL: Resetting lost threads to queue status if any"
        #logging.info(msg)
        args = {'componentName' : compName, \
            'thread_pool_id' : self.threadPoolId}
        self.query.updateWorkStatus(args, self.poolTable)
        self.query.updateWorkStatus(args, self.poolTableBufferIn)
        self.query.updateWorkStatus(args, self.poolTableBufferOut)
        # get the lenghts of the queue as this our starting point.
        self.callQueue = self.countMessages()
        # we do commit as initalization falls outside the while loop
        # of the component.
        myThread.transaction.commit()


    def prepareSlave(self, slave):
        """
        Prepares a slave to make sure it accesses
        the proper tables and is associated to the
        correct pool.
        """
        slave.args['thread_pool_table'] = self.poolTable
        slave.args['thread_pool_table_buffer_in'] = self.poolTableBufferIn
        slave.args['thread_pool_table_buffer_out'] = self.poolTableBufferOut
        slave.args['thread_pool_id'] = self.threadPoolId
        slave.args['thread_pool_buffer_size'] = self.bufferSize
        slave.args['componentName'] = self.component.config.Agent.componentName
        slave.component = self.component
        return slave

    def countMessages(self):
        """
        Counts how many messages for this pool are in the database.
        This is used when the pool is initialized to deal with entries
        that might be made before a crash and to establish an in
        memory queue length
        """

        msgs = 0
        args = {'componentName' : self.component.config.Agent.componentName, \
            'thread_pool_id' : self.threadPoolId}
        msgs += self.query.getQueueLength(args, self.poolTable)
        msgs += self.query.getQueueLength(args, self.poolTableBufferIn)
        msgs += self.query.getQueueLength(args, self.poolTableBufferOut)
        return msgs

    def enqueue( self, key, *parameters ):
        """
        _enqueue_

        Add a new work item to the queue.
        This may result in threads being spawned if there are threads
        available.

        """
        self.lock.acquire()
        base64_encoder = base64.encodebytes if PY3 else base64.encodestring
        args = {'event': str(key),
                'component' : self.component.config.Agent.componentName,
                'payload' : base64_encoder(encodeUnicodeToBytes(pickle.dumps(parameters))),
                'thread_pool_id' : self.threadPoolId}
        myThread = threading.currentThread()
        myThread.transaction.begin()
        self.query.insertWork(args, self.poolTableBufferIn)
        # we need to commit here otherwise the thread transaction might not
        # see it. check if this buffer needs to be flushed.
        myThread.transaction.commit()
        myThread.transaction.begin()
        bufferSize = self.query.getQueueLength(\
            {'componentName' : self.component.config.Agent.componentName, \
             'thread_pool_id' : self.threadPoolId}, self.poolTableBufferIn)
        if bufferSize > self.bufferSize:
            self.query.moveWorkFromBufferIn(self.poolTableBufferIn, \
                self.poolTable)
        #FIXME: we should call the msgService finsih method here before
        #this commit so we know the event/payload is transferred to a thread.
        myThread.transaction.commit()
        #logging.info("THREADPOOL: Enqueued item")

        # enqueue the work item
        self.callQueue += 1
        # check if there is a slave in the queue (then resue it)
        thread = None
        if len( self.slaveQueue ):
            # There is an available server: spawn a thread
            slave = self.slaveQueue[0]
            del self.slaveQueue[0]
            # Increment the count of active threads
            self.activeCount += 1
            thread = threading.Thread( target = self.slaveThread, \
                args=(slave,) )
            thread.start()
        # check if we can instantiate more slaves.
        else:
            if self.activeCount < self.nrOfSlaves:
                # we can still create slaves.
                slave  = \
                    self.slaveFactory.loadObject(self.slaveName)
                slave = self.prepareSlave(slave)
                self.activeCount += 1
                thread = threading.Thread( target = self.slaveThread, \
                    args=(slave,) )
                thread.start()

        self.lock.release()

        #if thread != None:
            #thread.join()



    def slaveThread( self, slaveServer ):
        """
        _slaveThread_

        This thread executes the method for each work item.

        """
        #~ print "slave thread starting up"
        self.lock.acquire()
        assert( self.activeCount > 0 )
        exceptCount = 0
        while self.callQueue > 0:
            # Dequeue work
            # handle exceptions to deal with deadlock issues.
            slaveServer.sane = True
            try:
                key, parameters = slaveServer.retrieveWork()
                self.callQueue -= 1
                exceptCount = 0
            except Exception as ex:
                logging.error("Problem with retrieving work : %s", str(ex))
                logging.error("Trying to salvage it")
                slaveServer.sane = False
                exceptCount += 1
                #TODO: Fix this exception; it's not a good exception
                #It just dumps the problem if it's the last thread in the queue
                if self.callQueue == 1 and exceptCount > 5:
                    self.callQueue -= 1
                    logging.error("If we got here, we screwed up badly enough I'm dumping it")
            self.lock.release()
            #~ print "making a call..."
            if slaveServer.sane:
                results = slaveServer( *parameters )
                slaveServer.removeWork()
                self.lock.acquire()
                # FIXME: do we want to keep this in?
                self.resultsQueue.append( (key, results) )
                # Notify the main thread that results are available,
                # if it was waiting
                self.lock.notify()
            else:
                # sleep for some random time.
                time.sleep(random.randint(1, 5))
                self.lock.acquire()


        # This thread is all done: put it back in the queue
        #~ print "slave thread exiting"
        self.activeCount -= 1
        assert( self.activeCount >= 0 )
        self.slaveQueue.append( slaveServer )
        self.lock.release()
