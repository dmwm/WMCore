"""
_ThreadPool_

A class used for createing thread pools.
To use this you need to use the ThreadSlave class
"""


__revision__ = "$Id: ThreadPool.py,v 1.1 2008/09/04 12:30:16 fvlingen Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "fvlingen@caltech.edu"

import base64
import cPickle
import logging
import random
import threading
import time

from WMCore.ThreadPool.WorkQueue import ThreadPool as Queue
from WMCore.WMFactory import WMFactory

class ThreadPool(Queue):
    """
    _ThreadPool_
    
    A class used for creating persistent thread pools.
    To use this you need to use the ThreadSlave class
    """

    def __init__(self, slaveModule, component, threadPoolID= 'default', nrOfSlaves = 0):
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
        self.threadPoolId = 'tp_threadpool_'+self.component.args['componentName']+'_'+threadPoolID
        # if set to false the peristent thread pool table is created per threadpool id.
        self.oneQueue = True
        # the size of the buffer to minimize single inserts in large tables.
        self.bufferSize = 400
        # the maximum number of slaves (threads) we allow.
        self.nrOfSlaves = nrOfSlaves

        #FIXME: placeholder for passing the slave class later on.
        slaveModule = "WMCore.ThreadPool.ThreadSlave"
        self.slaveName = slaveModule.split(".")[-1]
 
        self.slaveFactory = WMFactory("slaveFactory",slaveModule[0:slaveModule.rfind(slaveModule.split(".")[-1])-1])
        myThread = threading.currentThread()
        myThread.transaction.begin()
        factory = WMFactory("threadPool", "WMCore.ThreadPool."+ \
            myThread.dialect)
        self.query = myThread.factory['threadPool'].loadObject("Queries")

        # check which tables we need to use. 
        self.poolTable = 'tp_threadpool'
        self.poolTableBufferIn = 'tp_threadpool_buffer_in'
        self.poolTableBufferOut = 'tp_threadpool_buffer_out'
        if not self.oneQueue:
            self.poolTable = self.threadPoolId
            self.poolTableBufferIn = 'tp_threadpool_'+self.component.args['componentName']+'_'+threadPoolID+'_buffer_in'
            self.poolTableBufferOut = 'tp_threadpool_'+self.component.args['componentName']+'_'+threadPoolID+'_buffer_out'
            self.query.insertThreadPoolTables(self.poolTable)
            self.query.insertThreadPoolTables(self.poolTableBufferIn)
            self.query.insertThreadPoolTables(self.poolTableBufferOut)
            
        # inform the slaves what tables and id to use.
        #for slave in self.slaveQueue:
        #    slave.args['thread_pool_table'] = self.poolTable
        #    slave.args['thread_pool_table_buffer_in'] = self.poolTableBufferIn
        #    slave.args['thread_pool_table_buffer_out'] = self.poolTableBufferOut
        #    slave.args['thread_pool_id'] = self.threadPoolId
        #    slave.args['thread_pool_buffer_size'] = self.bufferSize
           
        # restore any threads that might have been lost during a crash
        # de register thread in database so we do not need to restore it.
        logging.info("THREADPOOL: Resetting lost threads to queue status if any")
        self.query.updateWorkStatus({'componentName' : component.args['componentName'], 'thread_pool_id' : self.threadPoolId}, self.poolTable)
        self.query.updateWorkStatus({'componentName' : component.args['componentName'], 'thread_pool_id' : self.threadPoolId}, self.poolTableBufferIn)
        self.query.updateWorkStatus({'componentName' : component.args['componentName'], 'thread_pool_id' : self.threadPoolId}, self.poolTableBufferOut)
        # get the lenghts of the queue as this our starting point.
        self.callQueue = self.countMessages()
        # we do commit as initalization falls outside the while loop
        # of the component.
        myThread.transaction.commit()


    def prepareSlave(self, slave):
            slave.args['thread_pool_table'] = self.poolTable
            slave.args['thread_pool_table_buffer_in'] = self.poolTableBufferIn
            slave.args['thread_pool_table_buffer_out'] = self.poolTableBufferOut
            slave.args['thread_pool_id'] = self.threadPoolId
            slave.args['thread_pool_buffer_size'] = self.bufferSize
            return slave

    def countMessages(self):
        msgs = 0
        msgs += self.query.getQueueLength({'componentName' : self.component.args['componentName'], 'thread_pool_id' : self.threadPoolId}, self.poolTable)
        msgs += self.query.getQueueLength({'componentName' : self.component.args['componentName'], 'thread_pool_id' : self.threadPoolId}, self.poolTableBufferIn)
        msgs += self.query.getQueueLength({'componentName' : self.component.args['componentName'], 'thread_pool_id' : self.threadPoolId}, self.poolTableBufferOut)
        return msgs

    def enqueue( self, key, *parameters ):
        """
        _enqueue_
        
        Add a new work item to the queue.
        This may result in threads being spawned if there are threads
        available.

        """
        self.lock.acquire()
        args = {'event': str(key), 'component' : str(self.component.args['componentName']), 'payload' : base64.encodestring(cPickle.dumps(parameters)), 'thread_pool_id' : self.threadPoolId}
        myThread = threading.currentThread()
        myThread.transaction.begin()
        self.query.insertWork(args, self.poolTableBufferIn)
        # we need to commit here otherwise the thread transaction might not see it.
        # check if this buffer needs to be flushed.
        bufferSize = self.query.getQueueLength({'componentName' : self.component.args['componentName'], 'thread_pool_id' : self.threadPoolId}, self.poolTableBufferIn)
        if bufferSize > self.bufferSize:
            self.query.moveWorkFromBufferIn(self.poolTableBufferIn, self.poolTable) 
        #FIXME: we should call the msgService finsih method here before this commit
        #FIXME: so we know the event/payload is transferred to a thread. 
        myThread.transaction.commit()
        logging.info("THREADPOOL: Enqueued item")
        work = (key,parameters)

        # enqueue the work item
        self.callQueue += 1
        # check if there is a slave in the queue (then resue it)
        if len( self.slaveQueue ):
            # There is an available server: spawn a thread
            slave = self.slaveQueue[0]
            del self.slaveQueue[0]
            # Increment the count of active threads
            self.activeCount += 1
            thread = threading.Thread( target = self.slaveThread, args=(slave,) )
            thread.start()
        # check if we can instantiate more slaves.
        else:       
            if self.activeCount < self.nrOfSlaves:
                # WE can still create slaves.
                slave  = self.slaveFactory.loadObject(classname = self.slaveName, args= (self.component))
                slave = self.prepareSlave(slave)
                self.activeCount += 1
                thread = threading.Thread( target = self.slaveThread, args=(slave,) )
                thread.start()
    
        self.lock.release()

    def slaveThread( self, slaveServer ):
        """
        _slaveThread_
        
        This thread executes the method for each work item.

        """
        #~ print "slave thread starting up"
        self.lock.acquire()
        assert( self.activeCount > 0 )
        while self.callQueue > 0:
            # Dequeue work
            # handle exceptions to deal with deadlock issues.
            slaveServer.sane= True
            try:
                key, parameters =slaveServer.retrieveWork()
                self.callQueue -= 1
            except Exception,ex:
                logging.error("Problem with retrieving work : "+str(ex))
                logging.error("Trying to salvage it")
                slaveServer.sane= False
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
                time.sleep(random.randint(1,5)) 
                self.lock.acquire()
                

        # This thread is all done: put it back in the queue
        #~ print "slave thread exiting"
        self.activeCount -= 1
        assert( self.activeCount >= 0 )
        self.slaveQueue.append( slaveServer )
        self.lock.release()

