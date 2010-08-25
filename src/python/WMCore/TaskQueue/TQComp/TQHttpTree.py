#!/usr/bin/env python

"""
_HttpTree_

This modules contains a class which maps HTTP service endpoints
to python methods. It defines the behaviour of cherrypy for incoming
requests. The main method is 'msg', which will decode requests as 
messages and act on them according to configuration (see the 'RemoteMsg'
module documentation).
"""

__revision__ = "$Id: TQHttpTree.py,v 1.1 2009/04/24 09:59:15 delgadop Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "antonio.delgado.peris@cern.ch"

import threading

import logging
from cherrypy import expose, request
from cherrypy.lib.static import serve_file
from CommonUtil import undojson
from Constants import sandboxUrlDir, specUrlDir

from WMCore.Database.Transaction import Transaction

class HttpTree(object):
    """ 
    _HttpTree_     
    """
    def __init__(self, params):
        """
        Constructor. 
 
        The required params are as follow:
          handlerMap, formatter, dbFactory, dialect,
          sandboxBasePath, specBasePath
        """ 
        required = ("handlerMap", "formatter", "dbFactory", "dialect", \
                    "sandboxBasePath", "specBasePath")

        for param in required:
            if not param in params:
                messg = "HttpTree object requires params['%s']" % param
                raise ValueError(messg)

        self.params = params

#        self.formatter = params["formatter"]
 
        self.mylogger = logging.getLogger()
 
        
    def initInThread(self):
        """
        Copy necessary things to our thread, so that handlers can take it
        """
        self.mylogger.debug("Running HttpTree.initInThread")
        myThread = threading.currentThread()
        if not hasattr(myThread, "dbFactory"):
            self.mylogger.debug("...copying dbFactory")
            myThread.dbFactory = self.params['dbFactory']
            myThread.dialect = self.params['dialect']
            myThread.logger = self.mylogger
        if not hasattr(myThread, "dbi"):
            self.mylogger.debug("...copying transaction")
            myThread.dbi = myThread.dbFactory.connect()
            myThread.transaction = Transaction(myThread.dbi)


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
    def static(self, *params):
#        self.mylogger.debug("Asked for %s" % params[0])
        if params[0] == sandboxUrlDir:
            localfile = self.params['sandboxBasePath']+'/'+params[1]
            self.mylogger.debug("Serving %s" % localfile)
            return serve_file(localfile)
        if params[0] == specUrlDir:
            self.mylogger.debug("Serving %s" % params[1])
            localfile = self.params['specBasePath']+'/'+params[1]
            self.mylogger.debug("Really serving %s" % localfile)
            return serve_file(localfile)
       
    
    @expose 
    def msg(self, msgType = None, payload = None, **kwd):
        """
        Defines the endpoint for the message service. Requests on it will
        be interpreted as messages that will be handled.
        """
        self.initInThread()
        
        sync = True
        if kwd.has_key('sync'):
            if kwd['sync'] == 'False': 
                sync = False
           
        try:
            payload = undojson(payload)
        except Exception, inst:
            messg = "Can't unjson msg payload: %s" % payload
            self.mylogger.error(messg)
            self.mylogger.error("Skiping message")
            data =   {'msgType':'Error', \
                      'payload': {'Error': messg}}
            return self.params['formatter'].format({'msg': data})
     
        handled = False
        for known in self.params['handlerMap']:
            if msgType == known:
                handled = True
                self.mylogger.debug("Handling msg of type %s and payload: %s."%
                     (msgType, payload))
#               result = self.handlerMap[msgType].__call__(msgType, payload)
                if sync:
                    handler = self.params['handlerMap'][msgType]
                    data = handler.__call__(msgType, payload)
                    return self.params['formatter'].format({'msg': data})
                else:
                    data = {'msgType': 'MessageReceived', 'payload': \
                             {'info': {'type': msgType, 'payload': payload}}}
                    request.hooks.attach('on_end_request', \
                          handler, failsafe=None, \
                          priority=None, msgType = msgType, payload = payload)

        if not handled:
            messg = "Wrong message format, or unknown msgType: %s" % msgType
            self.mylogger.debug(messg)
            self.mylogger.debug("Ignoring")
            data =   {'msgType':'Error', \
                      'payload': {'Error': messg}}
 
        return self.params['formatter'].format({'msg': data})
