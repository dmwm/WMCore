#!/usr/bin/env python
"""
__ThreadSlave__

Base class for implementing slave objects.
slave objects inherit from this and overload
the __call__ method. This method is (usually) passed
a list of parmeters used to complete a certain tasks.
parameters is an object that is pickled into the database
when it is not processed by a thread. It is thus possible
to pass arbitrary objects if needed through the parameters
attribute.
"""





from future import standard_library
standard_library.install_aliases()
from builtins import str, object

import base64
import logging
import threading
import os
import pickle

from WMCore.Database.Transaction import Transaction
from WMCore.WMFactory import WMFactory

from Utils.PythonVersion import PY3
from Utils.Utilities import encodeUnicodeToBytes

class ThreadSlave(object):
    """
    __ThreadSlave__

    Base class for implementing slave objects.
    slave objects inherit from this and overload
    the __call__ method. This method is (usually) passed
    a list of parmeters used to complete a certain tasks.
    parameters is an object that is pickled into the database
    when it is not processed by a thread. It is thus possible
    to pass arbitrary objects if needed through the parameters
    attribute.
    """

    def __init__(self ):
        """
        The constructor creates separate instances
        for objects that we do not want to share.
        Objects that are used as a thread slave
        Inherit from this to ensure they do not
        conflict with these methods.

        If needed the developer can add additional
        objects as it sees fit in the threadslave
        class that inherits from this.
        """
        self.args = {}
        # we also keep a reference to our component (we can
        # use this for read only things in the argument list).
        # assign this later
        self.component = None
        #a slave is created in its master thread so we can exploit
        #this to get a reference to its dbfactory object.
        myThread = threading.currentThread()
        self.dbFactory = myThread.dbFactory
        # get the procid from the mainthread msg service
        # if we use this in testing it might not be there.
        self.procid = 0

        # we can potentially use mapping from messages to handlers
        # to have one thread handling multiple message types.
        self.messages = {}
        # start loading some objects we need in this thread only
        factory = WMFactory("threadPool", "WMCore.ThreadPool."+ \
            myThread.dialect)
        self.query = factory.loadObject("Queries")

        #NOTE: this is not a new thread so we need to propagate
        #NOTE: the arguments we want to carry over to the current thread
        #NOTE: object using the initInThread method later on.

    def initInThread(self):
        """
        ThreadSlave (subclasses) are returned to a queue when no work
        is available and can be activated by a new thread when work
        is entered in the queue. This method is called when the ThreadSlave
        (subclass) is associated to a new thread.
        """

        # we need to call this method only when it is called within a thread
        # otherwise these parameters are not accissible in the thread used
        # to call this threadslave.

        myThread = threading.currentThread()
        # we are now in our own thread, so we can pass the dbFactory reference
        # to here:
        myThread.dbFactory = self.dbFactory

        if self.component.config.CoreDatabase.dialect:
            myThread.dialect = self.component.config.CoreDatabase.dialect
        else:
            # FIXME we aren't using the DIALECT environment variable anymore
            myThread.dialect = os.getenv("DIALECT")

        #TODO: remove as much as possible logging statements or make them debug
        myThread.logger = logging.getLogger()

        logging.info("THREAD: Initializing default database")
        logging.info("THREAD: Check if connection is through socket")
        logging.info("THREAD: Building database connection string")
        # we ensured that we use the dbFactory object from our parent
        # thread so we have only one engine in the application.
        myThread.dbi = myThread.dbFactory.connect()
        logging.info("THREAD: Initializing default transaction")
        myThread.transaction = Transaction(myThread.dbi)
        logging.info("THREAD: Loading backend")
        logging.info("THREAD constructor finished")

    # FIXME: enable the (configurable) possibility to retrieve
    # work in bulk (handy when we need to update databases)
    def retrieveWork(self):
        """
        _retrieveWork_

        If activated this threadlsave retrieves work. It retrieves
        it from the persistent thread pool and changes the state
        from queued to process.
        """
        myThread = threading.currentThread()
        # we only want to intitiate thread related issues once per thread.
        # this checks if our thread has the dbi attributes.
        if not hasattr(myThread,"dbi"):
            self.initInThread()
        else:
            # init creates a transaction that will call begin.
            myThread.transaction.begin()
        args = {'thread_pool_id' : self.args['thread_pool_id'], \
            'component' : self.args['componentName']}
        result = self.query.selectWork(args, \
            self.args['thread_pool_table_buffer_out'])
        # we might need to look into multiple buffers and move work to find it.
        # from keeping track of the number of messages for us we know it
        # is there.
        if result[0] == None:
            self.query.moveWorkToBufferOut(args, \
                self.args['thread_pool_table'], \
                self.args['thread_pool_table_buffer_out'], \
                self.args['thread_pool_buffer_size'])
        result = self.query.selectWork(args, \
            self.args['thread_pool_table_buffer_out'])
        if result[0] == None :
            self.query.moveWorkToBufferOut(args, \
                self.args['thread_pool_table_buffer_in'], \
                self.args['thread_pool_table_buffer_out'], \
                self.args['thread_pool_buffer_size'])
        result = self.query.selectWork(args, \
                self.args['thread_pool_table_buffer_out'])

        if result[0] == None:
            # FIXME: make proper exception
            raise Exception("ERROR: How can that be!!")
        logging.debug("THREAD: Retrieved Work with id: "+str(result[0]) )
        myThread.workId = str(result[0])
        # get the actual work now:
        result = self.query.retrieveWork({'id':myThread.workId}, \
            self.args['thread_pool_table_buffer_out'])
        self.query.tagWork({'id' : myThread.workId}, \
            self.args['thread_pool_table_buffer_out'])
        # we commit here because if the component crashes this is where
        # if will look for lost threads (the ones that are in the process state
        myThread.transaction.commit()
        base64_decoder = base64.decodebytes if PY3 else base64.decodestring
        return (result[1], pickle.loads(base64_decoder(encodeUnicodeToBytes(result[2]))))

    def removeWork(self):
        """
        _removeWork_

        Once the work is finished the entry is removed from the queue.
        """
        myThread = threading.currentThread()
        myThread.transaction.begin()
        self.query.removeWork({'id' : myThread.workId}, \
            self.args['thread_pool_table_buffer_out'])
        # this method is called once the thread is finished and
        # we commit everything.
        myThread.transaction.commit()

        logging.debug("THREAD: Removed Work")

    def __call__(self, parameters):
        """
        _call_

        components using a (or multiple) threadpools overload
        this method. They inherit from this class and make sure
        its constructor is called, and overload this call method.
        """
        logging.error("I am a placeholder please overload me to handle parameters : %s", str(parameters))
