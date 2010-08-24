#!/usr/bin/env python

"""
_PilotMonitorComponent_



"""





import os
import time
import cPickle
import threading
import logging

from WMCore import Configuration
# loaded from the config file.
from WMCore.WMFactory import WMFactory
from WMCore.Agent.Harness import Harness
import ProdAgent

from TQComp.Apis.TQStateApi import TQStateApi

class PilotMonitorComponent(Harness):
    """ 
    _PilotMonitorComponent_ 
    
    """
    def __init__(self, config):
        Harness.__init__(self, config)
        if ( not hasattr(self.config.PilotMonitorComponent,"pollInterval" ) ):
            self.pollInterval = "00:20:00"
        else:
            self.pollInterval = self.config.PilotMonitorComponent.pollInterval
 
	
    def preInitialization(self):
    
        if not hasattr(self.config.PilotMonitorComponent, "pilotMonitorHandler"):
	    self.config.PilotMonitorComponent.pilotMonitorHandler = \
                   'PilotMonitor.Handler.PilotMonitorHandler'
	
	factory = WMFactory("PilotMonitor");
	self.messages["MonitorPilots"] = factory.loadObject(\
                            self.config.PilotMonitorComponent.pilotMonitorHandler, self)
        
        tqconfig = Configuration.loadConfigurationFile( \
                   self.config.PilotMonitorComponent.TQConfig )

        myThread = threading.currentThread()
        self.logger = myThread.logger
        self.logger.debug( tqconfig)
        self.tqStateApi = TQStateApi(self.logger, tqconfig, None)


    def postInitialization(self):
        """ 
        _postInitialization_

        """

        myThread = threading.currentThread()
        if ( myThread.msgService != None):
            logging.debug('POST INITIALIZATION within IF')
            msgPayload = cPickle.dumps({'bulkSize':1, 'submissionMethod':'LSF'})
            msg = {'name':'MonitorPilots', \
                    'payload':msgPayload, \
                    'instant': False, \
                    'delay':'00:01:00' }
            myThread.msgService.publish(msg)
        else:
            logging.debug('POST INITIALIZATION: could not publish initial PilotMonitor message ')  
