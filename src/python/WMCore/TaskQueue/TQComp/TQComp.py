#!/usr/bin/env python

"""
Main component of the Task Queue
"""

__revision__ = "$Id: TQComp.py,v 1.3 2009/06/01 09:57:08 delgadop Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "antonio.delgado.peris@cern.ch"

import os
import time
import inspect
import threading

#from MessageService.MessageService import MessageService
from WMCore.Agent.Harness import Harness
# loaded from the config file.
from WMCore.WMFactory import WMFactory
from WMCore.WMException import WMException

# My class managing cherrypy
#from TQComp.TQListener import TQListener
from TQListener import TQListener
import Defaults

#for logging
import logging

class TQComp(Harness):
    """ 
    Main component of the Task Queue.
    """

    def __init__(self, config):
        """
        Constructor. Passes config to its parent WMCore.Agent.Harness.
        It also checks that some required configuration options are 
        present:
          downloadBaseUrl, sandboxBasePath, specBasePath, reportBasePath
        """
        Harness.__init__(self, config)
        print (config)
        
        required = ["downloadBaseUrl", "sandboxBasePath", \
                    "specBasePath", "reportBasePath"]
        for param in required:
            if not hasattr(self.config.TQComp, param):
                messg = "%s required in TQComp configuration" % param
                # TODO: What number?
                numb = 0
                raise WMException(messg, numb)
      
	
    def preInitialization(self):

        ######
        # Add handlers to messages
        ######
      
#        #  Factory to dynamically load handlers
#        factory = WMFactory('generic');
        pass




    def postInitialization(self):
			    
        ######
        # Create listener, set its handlers, start it
        ######
        self.listener = TQListener(self.config)

        # Get a few refs to utils from myThread to have them there for APIs
        myThread = threading.currentThread()
        self.transaction = myThread.transaction
        self.dialect = myThread.dialect

        # registerRequest handler
        params = {}
        self.listener.setHandler('registerRequest', \
           'TQComp.ListenerHandler.RegisterRequestHandler', params)

        # addFile handler
        params = {}
        self.listener.setHandler('addFile', \
           'TQComp.ListenerHandler.AddFileHandler', params)
           
        # fileRemoved handler
        params = {}
        self.listener.setHandler('fileRemoved', \
           'TQComp.ListenerHandler.FileRemovedHandler', params)

        # getTask handler
        params = {}
        params['downloadBaseUrl'] = self.config.TQComp.downloadBaseUrl
        params['sandboxBasePath'] = self.config.TQComp.sandboxBasePath
        params['specBasePath'] = self.config.TQComp.specBasePath
        if hasattr(self.config.TQComp, 'matcherPlugin'):
            params['matcherPlugin'] = self.config.TQComp.matcherPlugin
        else:
            params['matcherPlugin'] = Defaults.matcherPlugin
        self.listener.setHandler('getTask', \
           'TQComp.ListenerHandler.GetTaskHandler', params)
           
        # taskEnd handler
        params = {}
        if hasattr(self.config.TQComp, 'uploadBaseUrl'):
            params['uploadBaseUrl'] = self.config.TQComp.uploadBaseUrl
        else:
            params['uploadBaseUrl'] = self.config.TQComp.downloadBaseUrl
        params['specBasePath'] = self.config.TQComp.specBasePath
        self.listener.setHandler('taskEnd', \
           'TQComp.ListenerHandler.TaskEndHandler', params)

        # heartbeatHandler handler
        params = {}
        self.listener.setHandler('heartbeat', \
           'TQComp.ListenerHandler.HeartbeatHandler', params)

        # pilotShutdown handler
        params = {}
        self.listener.setHandler('pilotShutdown', \
           'TQComp.ListenerHandler.PilotShutdownHandler', params)



        # Start listener
        self.listener.startHttpServer()
      
      
    def prepareToStop(self, wait = False, stopPayload = ""):
        """
        _stopComponent

        Completing default behaviour of Harness class. Stopping the 
        listener previously.
        """
        self.listener.__del__()
        Harness.prepareToStop(self, wait, stopPayload)
