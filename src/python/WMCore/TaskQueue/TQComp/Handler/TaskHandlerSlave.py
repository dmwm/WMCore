#!/usr/bin/env python
"""
Slave used for NewTask handler.
"""

__all__ = []



import logging
import threading
import time
import random


# inherit from our default slave implementation
from TQComp.Handler.ParentSlave import ParentSlave

class TaskHandlerSlave(ParentSlave):
    """
    The slave handler for a NewTask message
    """

    
    def initInThread(self):
        """
        Called during thread initialization. Loads the
        backend for this instance.
        """
        # make sure you instantiate the super class method.
        ParentSlave.initInThread(self)

        logging.debug("TaskHandlerSlave initialized")


    def __call__(self, parameters):
        logging.debug("I am the TaskHandler slave called with parameters: "\
            +str(parameters))

        # first call the super class to do the default work.
        ParentSlave.__call__(self, parameters)

        # Now handle the message
        myThread = threading.currentThread()
        if parameters['event'] in ['NewTask']:

            # Extract the task attributes
            # Here we should check that all arguments are given correctly...
            parts = parameters["payload"].split(",")  
            logging.debug('TaskHandler:NewTask:parts'+str(parts))

            # Extract sandbox path if not known yet
            sandbox = parts[1]
            if not sandbox:
                sandbox = self.__getSandbox__(parts[0])

            # Insert job and its characteristics in the database
            myThread.transaction.begin()
            self.queries.add(*parts)

            # Say how many we have
            print "Number of tasks in queue: %s" % (self.queries.count())
            myThread.transaction.commit()

        else:
            # unexpected message, scream!
            pass


        # Typical ending
        sleepTime = random.randint(0, 5)
        myThread = threading.currentThread()
        # we need to do this in our slave otherwise the failure
        # messages that might have been published, will not be send.
        myThread.msgService.finish()

        time.sleep(sleepTime) 


    def __getSandbox__(self, xmlfile):
        dom = xml.dom.minidom.parse(xmlfile)
        params = dom.getElementsByTagName("JobSpec")[0].getElementsByTagName("Parameter")
        for param in params:
            if param.getAttribute("Name") == "BulkInputSandbox":
                result = self.__getText__(param.childNodes)
        return result


    def __getText__(self, nodelist):
        rc = ""
        for node in nodelist:
            if node.nodeType == node.TEXT_NODE:
                rc = rc + node.data
        return rc


