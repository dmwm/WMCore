#!/usr/bin/env python

"""
_SimpleInCompHandler_

This module contains a class that is an example of handler for incoming
remote messages for a RemoteMsg instance used within a WMCore component.
It is similar to the 'SimpleHandler', but shows how to take advantage
of the WMCore DB interface.

It could be mapped to a msgType using the 'RemoteMsg.setHandler' method.
"""





#import os
#import inspect

import threading 
import time
from WMCore.WMFactory import WMFactory

class SimpleInCompHandler(object):
    """ 
    _SimpleInCompHandler_

    Handles the msg with payload, and has access to WMCore comp DB
    by using self.query and myThread.transaction.
    """
    def __init__(self, params = None):
        myThread = threading.currentThread()
        self.mylogger = myThread.logger
        if params:
            if params.has_key("component"):
                self.myComp = params["component"]
                self.dbFactory = myThread.dbFactory
                self.dialect = myThread.dialect
                self.factory = WMFactory("default", self.myComp + \
                                       ".Database."+ self.dialect)
                self.queries = self.factory.loadObject("Queries")
       

    def __call__(self, msgType, payload):
        """
        Handles the msg with payload, and has access to WMCore comp DB
        by using self.query and myThread.transaction.
       
        You could do things like:
       
          myThread = threading.currentThread()
          myThread.transaction.begin()
          self.queries.updateTask(taskId, vars)
          self.mylogger.debug("Task updated as Done.")
          myThread.transaction.commit()
        """

        self.mylogger("\nSimpleHandler acting on message: %s" % (time.ctime()))
        # Simulate that it takes some time...
        time.sleep(2)
        self.mylogger("\nSimpleHandler received: msgType: %s" %(msgType))
        self.mylogger("                payload: "+payload)
        self.mylogger("Ended: SimpleHandler at %s" % (time.ctime()))
        
        result = ['This is an example of what can be returned', 0, {'dict': 'also'}]
        return result
