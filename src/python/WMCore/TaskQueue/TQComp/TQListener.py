#!/usr/bin/env python

"""
_TQListener_

"""

__revision__ = "$Id: TQListener.py,v 1.2 2009/04/30 09:00:23 delgadop Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "antonio.delgado.peris@cern.ch"

import os
import threading
import time
#import inspect
from logging.handlers import RotatingFileHandler

from WMCore.WMFactory import WMFactory
#from WMCore.Configuration import Configuration

# Classes from this package
#from Sender import Sender
from TQHttpServer import HttpServer
from TQHttpTree import HttpTree
from Defaults import listenerFormatter

#for logging
import logging


class TQListener(object):
    """ 
    _TQListener_ 
    
    """
    def __init__(self, config):
        """
        Constructor.
        
        Requires a WMCore.Configuration object with configuration 
        information. 
        """

        self.myconfig = config
     
        # Logging
        myThread = threading.currentThread()
#        self._setLogging()
        self.mylogger = myThread.logger
        self.mylogger.info("\n\n>>>>>TQListener object being created <<<<<<\n")
     
        sections = self.myconfig.listSections_()
        if not "TQListener" in sections:
            messg = "Cannot create TQListener object without "
            messg += "TQListener section in config file"
            raise Exception(messg)
     
        self.user = None
        self.passwd = None
     
        self.handlerMap = {}
        self.handlerLock = threading.Lock()
        self.factory = WMFactory('TQListener');
     
        # Get a reference to our invoking component's DB factory
        self.dbFactory = myThread.dbFactory
        self.dialect = myThread.dialect
     
      
        # Formatter for responses (change only if in synchronous mode)
        formatter = listenerFormatter
#        formatter = "TQComp.DefaultFormatter"
        if hasattr(self.myconfig.TQListener, "formatter"):
            formatter = self.myconfig.TQListener.formatter
        formatterObj = self.factory.loadObject(formatter)

        params = { "handlerMap": self.handlerMap,
                   "formatter": formatterObj, 
                   "dbFactory": self.dbFactory,
                   "dialect": self.dialect,
                   "sandboxBasePath": self.myconfig.TQComp.sandboxBasePath,
                   "specBasePath": self.myconfig.TQComp.specBasePath,
                   "reportBasePath": self.myconfig.TQComp.reportBasePath
                 }
       
        self.httpTree = HttpTree(params)
        self.httpServer = None


    def __del__(self):
        # Tell cherrypy to die
        self.mylogger.info("Asking httpServer to die")
        if self.httpServer:
            self.httpServer.terminate()
            self.httpServer.join()


    def startHttpServer(self):
        """
        Starts an HTTP server that listens to incoming remote messages.
        The method will refuse to create a server if there is already one
        """

        if not self.httpServer:
            self.mylogger.info("Starting HttpServer")
            self.httpServer = HttpServer(self.httpTree, self.myconfig)
            self.httpServer.start()
        else:
            messg = "Refusing to start HTTP server (there is already one)."
            self.mylogger.info(messg)


    def setAuthentication(self, user, passwd):
        """
        Sets the user/password for authentication
        with the remote application. Has to be done
        before sending a message.
        """
        self.mylogger.debug("Setting user and passwd")
        self.user = user
        self.passwd = passwd


    def setHandler(self, msgType, handler, params):
        """
        Maps the specified handler to the indicated message type.
        The handler must be the name of a class which can be called (e.g.
        RemoteMsg.SimpleHandler). The params argument can be used as a 
        dict for any parameter (if needed); it will be passed to 
        the constructor of the handler.
        """
        messg = "Setting new handler %s for msgType %s" % (handler, msgType)
        self.mylogger.debug(messg)
        #  Factory to dynamically load handlers
        newHandler = self.factory.loadObject(handler, params)
        self.handlerLock.acquire()
        self.handlerMap[msgType] = newHandler
        self.handlerLock.release()

       

#   def _setLogging(self):
#      compSect = self.myconfig.TQListener

#      # Logging
#      if not hasattr(compSect, "logFile"):
#          compSect.logFile = os.path.join(compSect.componentDir, \
#              "comp.log")
#      print('Log file is: '+compSect.logFile)

#      if not hasattr(compSect, "listenerLogFile"):
#          compSect.listenerLogFile = os.path.join(compSect.componentDir, \
#              "listener.log")
#      print('Listener log file is: '+compSect.listenerLogFile)

#      logHandler = RotatingFileHandler(compSect.logFile,
#          "a", 1000000, 3)
#      logFormatter = \
#          logging.Formatter("%(asctime)s:%(levelname)s:%(filename)s:%(message)s")
#      logHandler.setFormatter(logFormatter)
#      self.mylogger = logging.getLogger("TQListener")
##      logging.getLogger("").addHandler(logHandler)
##      logging.getLogger("").setLevel(logging.INFO)
#      self.mylogger.addHandler(logHandler)
#      self.mylogger.setLevel(logging.INFO)
#      # map log strings to integer levels:
#      self.logMsg = {'DEBUG' :   logging.DEBUG,    \
#                    'ERROR' :   logging.ERROR,     \
#                    'NOTSET':   logging.NOTSET,    \
#                    'CRITICAL' : logging.CRITICAL, \
#                    'WARNING'  : logging.WARNING,  \
#                    'INFO'     : logging.INFO,     }
##                    'SQLDEBUG' : logging.SQLDEBUG  }
#      if hasattr(compSect, "logLevel") and \
#         compSect.logLevel in self.logMsg.keys():
##          logging.getLogger().setLevel(self.logMsg[compSect.logLevel])
#          self.mylogger.setLevel(self.logMsg[compSect.logLevel])



