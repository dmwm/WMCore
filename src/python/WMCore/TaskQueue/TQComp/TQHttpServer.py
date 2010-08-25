#!/usr/bin/env python

"""
_Listener_

This module contains a class that is responsible for starting (and 
terminating) a cherrypy server that listens to incoming remote
messages. Behaviour of this server is managed by a HttpTree object.
"""

__revision__ = "$Id: TQHttpServer.py,v 1.1 2009/04/24 09:59:15 delgadop Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "antonio.delgado.peris@cern.ch"

#import os
#import inspect

from threading import Thread
import time
import logging
import cherrypy
from Defaults import listenerMaxThreads

#from Constants import sandboxBasePath, sandboxBaseUrl

class HttpServer(Thread):
    """ 
    _Listener_ 
    
    """
    def __init__(self, httpTree, config):

        Thread.__init__(self)
      
        self.exitFlag = False
        # TODO: why not pass the logger in __init__
        self.mylogger = logging.getLogger()
#        self.mylogger = logging.getLogger("TQListener")
      
        self.config = config.TQListener

        self.configFile = None
        if hasattr(self.config, "httpServerConfig"):
            self.configFile = self.config.httpServerConfig
#        print self.configFile


        # Config dictionary that will be passed to cherrypy
        self.configDict = {}
        
        # Maximum number of threads for cherrypy
        self.maxThreads = listenerMaxThreads
        if hasattr(self.config, "maxThreads"):
            self.configDict["server.thread_pool"] = self.config.maxThreads
            self.maxThreads = self.config.maxThreads

        # Log file
        self.configDict["log.screen"] = False
        if hasattr(self.config, "logFile"):
            logFile = self.config.httpServerLogFile
            self.configDict["log.access_file"] = logFile
            self.configDict["log.error_file"] = logFile

        # Port
        if hasattr(self.config, "httpServerPort"):
            port = int(self.config.httpServerPort)
            self.configDict["server.socket_port"] = port
            messg = "TQListener Listener listening on port %s" %(port)
            self.mylogger.info(messg)

        # If user is specified, we want user-pwd authentication
        if hasattr(self.config, "httpServerUser"):
            user = self.config.httpServerUser
            self.configDict['tools.digest_auth.on'] = 'True'
            pwd = None
            if hasattr(self.config, "httpServerPwd"):
                pwd = self.config.httpServerPwd
            self.configDict['tools.digest_auth.users'] = {user: pwd}
            # Include realm if specified
            if hasattr(self.config, "httpServerRealm"):
                self.configDict['tools.digest_auth.realm'] = parameters["realm"]

# Following would be for basic authentication (not digest)
# But does not seem to work...
#      self.configDict['tools.basic_auth.on'] = 'True'
#      pwd = None
#      self.configDict['tools.basic_auth.users'] = {user: pwd}
#      self.configDict['tools.basic_auth.realm'] = parameters["realm"]
#      def clear_text(passwd):
#          return passwd
#      self.configDict['tools.basic_auth.encrypt'] = clear_text


        # Instantiate the HTTP Tree object
        if httpTree:
            self.httpTree = httpTree
        else:
            try:
                from TQHttpTree import HttpTree
                self.httpTree = HttpTree()
            except:
                msg = "Could not load any HttpTree object"
                self.mylogger.error(msg)
                raise Exception(msg)
         
#        self.configDict['/'] = {'tools.staticdir.root': '/'}
#        self.configDict['/'+sandboxBaseUrl] = {'tools.staticdir.on': True,
#                                  'tools.staticdir.dir':sandboxBasePath}
    
    def run(self):
        """
        Main method of the thread. It will be called when started.
        It starts a cherrypy server to listen for incoming messages.
        """
        cherrypy.engine.SIGHUP = None
        cherrypy.engine.SIGTERM = None
        cherrypy.engine.autoreload_on = False

        # User config file if specified
        if self.configFile:
            cherrypy.config.update(self.configFile)
        # Override explicitly passed config options
        cherrypy.config.update(self.configDict)
        
        cherrypy.tree.mount(self.httpTree)
        cherrypy.server.quickstart()
        cherrypy.engine.start(blocking=False)
      
        # Loop till done
        finished = False
        while not finished:
            time.sleep(5)
            finished = self.exitFlag
       
        # When done, exit gracefully
        self._suicide()


    def terminate(self):
        """
        Invoked to make this thread stop the cherrypy server and exit.
        """
        # Just set the flag for the thread to read afterwards
        self.exitFlag = True


    def _suicide(self):
        """
        Called by the main loop when 'self.exitFlag' becomes True to stop
        the cherrypy server before exiting.
        """
        self.mylogger.info("Stopping cherrypy engine and server")
        cherrypy.engine.stop()
        cherrypy.server.stop()
        self.mylogger.info("All done. Goodbye")

