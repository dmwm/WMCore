#!/usr/bin/env python

"""
Main component of the Task Queue
"""

__revision__ = "$Id: TQComp.py,v 1.2 2009/04/30 09:00:22 delgadop Exp $"
__version__ = "$Revision: 1.2 $"
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
                    "specBasePath", "reportBasePath" ]
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
      
        #  Factory to dynamically load handlers
        factory = WMFactory('generic');

        # New tasks could theoretically arrive by message
        # But we will support API only

#        if not hasattr(self.config.TQComp, "taskHandler"):
#            self.config.TQComp.taskHandler = 'TQComp.Handler.TaskHandler'

#        self.messages["NewTask"] = factory.loadObject(\
#                                self.config.TQComp.taskHandler, self)
      
#        logging.debug("messages is: %s" % self.messages)
        
# There will be no msg for a pilot request (if this comp is managing cherrypy himself)
#        self.messages["PilotRequest"] = factory.loadObject(\
#                             self.config.TQComp.taskHandler, self)



    def postInitialization(self):
			    
        ######
        # Create listener, set its listeners, start it
        ######
        self.listener = TQListener(self.config)

        # Get a few refs to utils from myThread to have them there for APIs
        myThread = threading.currentThread()
        self.transaction = myThread.transaction
        self.dialect = myThread.dialect

        params = {}
        params['downloadBaseUrl'] = self.config.TQComp.downloadBaseUrl
        params['sandboxBasePath'] = self.config.TQComp.sandboxBasePath
        params['specBasePath'] = self.config.TQComp.specBasePath
        self.listener.setHandler('getTask', \
           'TQComp.ListenerHandler.GetTaskHandler', params)
           
        params = {}
        if hasattr(self.config.TQComp, 'uploadBaseUrl'):
            params['uploadBaseUrl'] = self.config.TQComp.uploadBaseUrl
        else:
            params['uploadBaseUrl'] = self.config.TQComp.downloadBaseUrl
        params['specBasePath'] = self.config.TQComp.specBasePath
        self.listener.setHandler('taskEnd', \
           'TQComp.ListenerHandler.TaskEndHandler', params)
        self.listener.startHttpServer()
      


    def prepareToStop(self, wait = False, stopPayload = ""):
        """
        _stopComponent

        Completing default behaviour of Harness class. Stopping the 
        listener previously.
        """
        self.listener.__del__()
        Harness.prepareToStop(self, wait, stopPayload)
