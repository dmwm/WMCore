#!/usr/bin/env python

"""
_HttpTree_

This modules contains a class which maps HTTP service endpoints
to python methods. It defines the behaviour of cherrypy for incoming
requests. The main method is 'msg', which will decode requests as 
messages and act on them according to configuration (see the 'RemoteMsg'
module documentation).
"""

__revision__ = "$Id: TQHttpTree.py,v 1.2 2009/04/30 09:00:23 delgadop Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "antonio.delgado.peris@cern.ch"

import threading

import logging
import sys
from cherrypy import expose, request
from cherrypy.lib.static import serve_file
from CommonUtil import undojson
from Constants import sandboxUrlDir, specUrlDir, reportUrlDir

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
          sandboxBasePath, specBasePath, reportBasePath
        """
        # TODO:
        required = ("handlerMap", "formatter", "dbFactory", "dialect", \
                    "sandboxBasePath", "specBasePath", "reportBasePath")
#        required = ("handlerMap", "formatter", "dbFactory", "dialect", \
#                    "sandboxBasePath", "specBasePath")

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
    def upload(self, *params):
        self.mylogger.debug("Received upload request with params as follows")

        # TODO: Delete next line
#        self.params['reportBasePath'] = '/pool/TaskQueue/playground/reports'
        if params[0] == reportUrlDir:
            subpath = ""
            for p in params[1:]:
                self.mylogger.debug("Param: %s" % p)
                subpath = subpath + '/' + p
            path = self.params['reportBasePath'] + subpath
            try:
                # Note: The parent dirs must be created beforehand by TQ
                # e.g. when job was enqueued, or when it's assigned to a pilot
                # Or: they are already there (by job creator or someone else)
                all = request.body.read()
                exists = False
                # If the file exists (can be opened), we raise an error
                try:
                   open(path)
                   exists = True
                except:
                   pass
                if exists:
                   raise Exception("Destination file exists. Can't overwrite!")
                f2 = open(path, 'wb')
                f2.write(all)
                f2.close()
                data = {'msgType':'FileStored', \
                        'payload': {'Path': subpath, 'Length': len(all)}}
                return self.params['formatter'].format({'msg': data})
            except:
                type, val, tb = sys.exc_info()
                messg = "Problem storing file %s: %s - %s" % (path, type, val)
                data = {'msgType': 'Error', \
                        'payload': {'Error': messg}}
                return self.params['formatter'].format({'msg': data})

           

    @expose
    def static(self, *params):
#        self.mylogger.debug("Asked for %s" % params[0])
        if params[0] == sandboxUrlDir:
            subpath = ""
            for p in params[1:]:
                self.mylogger.debug("Param: %s" % p)
                subpath = subpath + '/' + p
            path = self.params['sandboxBasePath'] + subpath
            self.mylogger.debug("Serving %s" % subpath)
            self.mylogger.debug("Really serving %s" % path)
            return serve_file(path)
        if params[0] == specUrlDir:
            subpath = ""
            for p in params[1:]:
                self.mylogger.debug("Param: %s" % p)
                subpath = subpath + '/' + p
            path = self.params['specBasePath'] + subpath
            self.mylogger.debug("Serving %s" % subpath)
            self.mylogger.debug("Really serving %s" % path)
            return serve_file(path)
       
    
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
