#!/usr/bin/env python

"""
_HttpTree_

This modules contains a class which maps HTTP service endpoints
to python methods. It defines the behaviour of cherrypy for incoming
requests. The main method is 'msg', which will decode requests as 
messages and act on them according to configuration (see the 'RemoteMsg'
module documentation).
"""

__revision__ = "$Id: HttpTree.py,v 1.1 2009/04/01 12:56:01 delgadop Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "antonio.delgado.peris@cern.ch"

import threading

import logging
from cherrypy import expose, request
from CommonUtil import undojson

from WMCore.Database.Transaction import Transaction

class HttpTree(object):
    """ 
    _HttpTree_     
    """
    def __init__(self, params):
        """
        Constructor. 
 
        The required params are as follow:
          msgQueue, handlerMap, msgLock, formatter
 
        The optional params are the following:
          component, dbFactory, dialect, queue (default True)
 
        If component is present, dbFactory and dialect must be
        also present.
        
        """ 

        for test in ("msgQueue", "handlerMap", "msgLock", "formatter"):
            if not test in params:
                msg = "'%s' is needed in 'params' for HttpTree object." % test
                raise ValueError(msg)
        self.msgQueue = params["msgQueue"]
        self.handlerMap = params["handlerMap"]
        self.msgLock = params["msgLock"]
        self.formatter = params["formatter"]
#        self.handlerLock = handlerLock
       
        self.queueMode = True
        if params.has_key("queueMode"):
            self.queueMode = params["queueMode"]

        self.myComp = None
        if params.has_key("component"):
            self.myComp = params["component"]
            self.dbFactory = params["dbFactory"]
            self.dialect = params["dialect"]
 
        self.mylogger = logging.getLogger("RemoteMsg")
 
        
    def initInThread(self):
        """
        Copy necessary things to our thread, so that handlers can take it
        """
        self.mylogger.debug("Running HttpTree.initInThread")
        myThread = threading.currentThread()
        if not hasattr(myThread, "dbFactory"):
            self.mylogger.debug("...copying dbFactory")
            myThread.dbFactory = self.dbFactory
            myThread.dialect = self.dialect
            myThread.logger = self.mylogger
        if not hasattr(myThread, "dbi"):
            self.mylogger.debug("...copying transaction")
            myThread.dbi = myThread.dbFactory.connect()
            myThread.transaction = Transaction(myThread.dbi)


    def setQueue(self, value):
        """
        Set queue mode: True -> queue, False -> handle messages as they arrive.
        """
        self.queueMode = value

    @expose 
    def index(self):
        """
        Default index page for our HTTP server. It just shows an informative
        message in case some browser reads it.
        """
        return """
<html><body>
<pre>
   Hello World!

   To send a message, please point to:
       http://server:port/msg?msgType=YOURTYPE&payload=YOURPAYLOAD
</pre>
</body></html>
       """

    @expose
    def msgList(self, **kwd):
        """
        Endpoint for message list. This method is not required. It is just 
        a utility that will return a list of the messages in the queue.
        """
        if self.myComp:
            self.initInThread()
       
        self.msgLock.acquire()
        result = []
        for msg in self.msgQueue:
            result.append(msg)
        self.msgLock.release()
        return self.formatter.format(result)
       
    
    @expose 
    def msg(self, msgType = None, payload = None, **kwd):
        """
        Defines the endpoint for the message service. Request on it will
        be interpreted as messages. They will be enqueued or handled, 
        depending on configuration.
        """
        if self.myComp:
            self.initInThread()
        
        sync = False
        if kwd.has_key('sync'):
            if kwd['sync'] == 'True': 
                sync = True
           
        try:
            payload = undojson(payload)
        except Exception, inst:
            result = "Can't unjson msg payload: %s" % payload
            self.mylogger.error(result)
            self.mylogger.error("Skiping message")
            info = "Can't unjson msg payload. Skiping message."
            data = {'msgType':msgType, 'payload':payload, \
                    'result':'Error', 'info':info}
            return self.formatter.format(data)
     
        # If queueMode = True, we should store there the message
        if self.queueMode:
            if msgType:
                self.mylogger.debug("Storing msg of type %s and payload: %s." %
                    (msgType, payload))
                self.msgLock.acquire()
                self.msgQueue.append([msgType, payload])
                self.msgLock.release()
            data = {'msgType': msgType, 'payload': payload, 'result': \
                    'Received', 'info': 'Message Enqueued'}
            return self.formatter.format(data)
     
        handled = False
        for known in self.handlerMap:
            if msgType == known:
                handled = True
                self.mylogger.debug("Handling msg of type %s and payload: %s."%
                     (msgType, payload))
                if sync:
                    data = self.handlerMap[msgType].__call__(msgType, payload)
                    return self.formatter.format(data)
                else:
                    result = "Received"
                    info = "Message handled"
                    request.hooks.attach('on_end_request', \
                          self.handlerMap[msgType], failsafe=None, \
                          priority=None, msgType = msgType, payload = payload)

        if not handled:
            msg = "Wrong message format, or unknown msgType: %s" % msgType
            self.mylogger.debug(msg)
            self.mylogger.debug("Ignoring")
            result = "Error"
            info = "Wrong message format, or unknown msgType!"
 
        data = {'msgType': msgType, 'payload': payload, \
                'result': result, 'info': info}
        return self.formatter.format(data)
