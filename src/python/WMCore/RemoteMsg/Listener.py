#!/usr/bin/env python

"""
_Listener_

This module contains a class that is responsible for starting (and 
terminating) a cherrypy server that listens to incoming remote
messages. Behaviour of this server is managed by a HttpTree object.
"""





#import os
#import inspect

from threading import Thread
import time
import logging
import cherrypy

class Listener(Thread):
    """ 
    _Listener_ 
    
    """
    def __init__(self, httpTree, config):

        Thread.__init__(self)

        self.exitFlag = False
        self.mylogger = logging.getLogger("RemoteMsg")

        self.config = config
        parameters = {}
        parameters["httpTree"] = httpTree
        parameters["logFile"] = self.config.RemoteMsg.listenerLogFile
        if hasattr(self.config.RemoteMsg, "listenerConfig"):
            parameters["configFile"] = self.config.RemoteMsg.listenerConfig
        if hasattr(self.config.RemoteMsg, "listenerPort"):
            parameters["port"] = self.config.RemoteMsg.listenerPort
        if hasattr(self.config.RemoteMsg, "listenerUser"):
            parameters["user"] = self.config.RemoteMsg.listenerUser
        if hasattr(self.config.RemoteMsg, "listenerPwd"):
            parameters["pwd"] = self.config.RemoteMsg.listenerPwd
        if hasattr(self.config.RemoteMsg, "listenerRealm"):
            parameters["realm"] = self.config.RemoteMsg.listenerRealm

        self.configFile = None
        if "configFile" in parameters:
            self.configFile = parameters["configFile"]

        self.configDict = {}
#      self.configDict["server.log_to_screen"] = False
        self.configDict["log.screen"] = False
        if "logFile" in parameters:
            self.configDict["log.access_file"] = parameters["logFile"]
            self.configDict["log.error_file"] = parameters["logFile"]
        if "port" in parameters:
            port = int(parameters["port"])
            self.configDict["server.socket_port"] = port
            self.mylogger.info("RemoteMsg Listener listening on port %s"%(port))
            print("RemoteMsg Listener listening on port %s" %(port))

        # If user is specified, we want user-pwd authentication
        if "user" in parameters:
            user = parameters["user"]
            self.configDict['tools.digest_auth.on'] = 'True'
            pwd = None
            if "pwd" in parameters:
                pwd = parameters["pwd"]
            self.configDict['tools.digest_auth.users'] = {user: pwd}
        if "realm" in parameters:
            self.configDict['tools.digest_auth.realm'] = parameters["realm"]

# Following would be for basic authentication (not digest)
# But does not seem to work...
#
#      if "user" in parameters:
#         user = parameters["user"]
#         self.configDict['tools.basic_auth.on'] = 'True'
#         pwd = None
#         if "pwd" in parameters:
#            pwd = parameters["pwd"]
#         self.configDict['tools.basic_auth.users'] = {user: pwd}
#      if "realm" in parameters:
#         self.configDict['tools.basic_auth.realm'] = parameters["realm"]
#      def clear_text(passwd):
#          return passwd
#      self.configDict['tools.basic_auth.encrypt'] = clear_text
#      print self.configDict
#
        if "httpTree" in parameters:
            self.httpTree = parameters["httpTree"]
        else:
            try:
                from HttpTree import HttpTree
                self.httpTree = HttpTree()
            except:
                msg = "Could not load any HttpTree object"
                self.mylogger.error(msg)
                raise Exception(msg)
         
      
    
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
        cherrypy.log._set_screen = False
        
        cherrypy.tree.mount(self.httpTree)
        cherrypy.server.quickstart()
        cherrypy.engine.start(blocking=False)
        
        cherrypy.log._set_screen = False

        # Loop till done
        finished = False
        while not finished:
            time.sleep(5)
            finished = self.exitFlag
      
        # When done, exit gracefully
        self.__suicide__()


    def terminate(self):
        """
        Invoked to make this thread stop the cherrypy server and exit.
        """
        # Just set the flag for the thread to read afterwards
        self.exitFlag = True


    def __suicide__(self):
        """
        Called by the main loop when 'self.exitFlag' becomes True to stop
        the cherrypy server before exiting.
        """
        self.mylogger.info("Stopping cherrypy engine and server")
        cherrypy.engine.stop()
        cherrypy.server.stop()
        self.mylogger.info("All done. Goodbye")

