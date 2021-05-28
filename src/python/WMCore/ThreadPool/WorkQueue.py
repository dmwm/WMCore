#!/usr/bin/env python
"""
_ThreadPool_

Thread based work queue that allows a set of slaves to be added to a queue
to process information in the queue in parallel and return the output
of each slave in an output queue.

Written by Evan Jones
http://evanjones.ca/software/python-workqueue.html

Worker queues for Python (also called thread pools).

This class queues work and distributes it over a set of slave objects. The
slaves are Callable instances that get called in their own thread. I use
it with XML-RPC server instances to distribute work over a cluster of
machines.

The enqueue() function is used to queue function calls. The dequeue() function
returns the results of actually calling the function. This permits the
execution of the function to happen in the background in another thread. The
order that results are returned by dequeue() is not the same as the order work
is placed into the queue with enqueue().

This class uses "keys" to track specific work items. When enqueuing a work
item, a key is also required. The key does not affect the processing at all.
It is simply returned when dequeue() returns the result of calling the
function. This helps the caller to determine which function

"""

from builtins import object
from future import standard_library
standard_library.install_aliases()

import threading

class SerializedThreadPool( object ):
    """
    Implements the ThreadPool interface in a serialized fashion. This
    is very useful for debugging since there is no parallelism. All the
    work is performed when calling dequeue.
    """

    __slots__ = ( 'function', 'queue' )
    def __init__( self, slaveFunction ):
        """
        Creates a new SerializedThreadPool that will call the
        specified function.
        """
        self.function = slaveFunction
        if hasattr( slaveFunction, '__len__' ):
            assert( len( slaveFunction ) == 1 )
            self.function = slaveFunction[0]

        self.queue = []

    def enqueue( self, key, *parameters ):
        """
        Adds a new work item to the queue.
        This does not actually execute the function.
        """
        self.queue.append( (key, parameters) )

    def dequeue( self ):
        """
        Returns a (key, result) pair from a completed work item,
        or None if there are no more items. This will actually
        cause the function to be executed.
        """

        if len( self.queue ) == 0:
            return None

        # Take from the tail (much faster)
        key, parameters = self.queue[-1]
        del self.queue[-1]

        result = self.function( *parameters )
        return key, result

    def __iter__( self ):
        """
        Returns an iterator over the (key, result) pairs
        returned by dequeue(). The iterator stops when there
        are no more results in the queue.
        """
        result = self.dequeue()
        while result != None:
            yield result
            result = self.dequeue()

class ThreadPool( object ):
    """
    _ThreadPool_

    Distributes work over a set of slaves. Each slave is a
    callable object that will be called from a seperate thread.
    """
    def __init__( self, slaves ):
        self.lock = threading.Condition()

        # Server objects that are currently unused
        self.slaveQueue = list( slaves )
        # Pending work
        self.callQueue = []
        # Pending results
        self.resultsQueue = []
        # Number of threads currently active
        self.activeCount = 0

    def enqueue( self, key, *parameters ):
        """
        _enqueue_

        Add a new work item to the queue.
        This may result in threads being spawned.

        """

        work = (key, parameters)

        self.lock.acquire()

        # enqueue the work item
        self.callQueue.append(work)

        if len( self.slaveQueue ):
            # There is an available server: spawn a thread
            slave = self.slaveQueue[0]
            del self.slaveQueue[0]

            # Increment the count of active threads
            self.activeCount += 1
            thread = threading.Thread( target = self.slaveThread, \
                args=(slave,) )
            thread.start()

        self.lock.release()

    def __iter__( self ):
        """
        Returns an iterator over the (key, result) pairs
        returned by dequeue(). The iterator stops when there
        are no more results in the queue.
        """
        result = self.dequeue()
        while result != None:
            yield result
            result = self.dequeue()

    def dequeue( self ):
        """
        _dequeue_

        Returns a completed work item from the queue,
        or None if there are no more items. If there
        are worker threads still working, this will
        block until results are available.
        """

        result = None

        self.lock.acquire()

        while True:
            if len( self.resultsQueue ):
                # There is a result in the queue: take it
                result = self.resultsQueue[0]
                del self.resultsQueue[0]
                break
            elif self.activeCount == 0:
                # If there are no active threads, then there
                # are no results pending: quit
                result = None
                break
            else:
                assert( self.activeCount > 0 )
                # No result in the queue: wait for work
                self.lock.wait()

        self.lock.release()

        return result

    def slaveThread( self, slaveServer ):
        """
        _slaveThread_

        This thread executes the method for each work item.

        """
        #~ print "slave thread starting up"
        self.lock.acquire()
        assert( self.activeCount > 0 )

        while len( self.callQueue ) > 0:
            # Dequeue work
            key, parameters = self.callQueue[0]
            del self.callQueue[0]

            self.lock.release()
            #~ print "making a call..."
            results = slaveServer( *parameters )
            self.lock.acquire()

            self.resultsQueue.append( (key, results) )
            # Notify the main thread that results are available,
            # if it was waiting
            self.lock.notify()

        # This thread is all done: put it back in the queue
        #~ print "slave thread exiting"
        self.activeCount -= 1
        assert( self.activeCount >= 0 )
        self.slaveQueue.append( slaveServer )
        self.lock.release()
