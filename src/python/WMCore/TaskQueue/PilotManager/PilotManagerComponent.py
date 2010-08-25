#!/usr/bin/env python

"""
_JobQueueComponent_



"""





import os
import time
import cPickle
import inspect
import threading
#import logging

from WMCore.Agent.Harness import Harness
# loaded from the config file.
from WMCore.Agent.Configuration import loadConfigurationFile
from WMCore.WMFactory import WMFactory


import ProdAgent
from ProdAgentCore.Configuration import loadProdAgentConfiguration
#from Handler.PilotManagerHandler import PilotManagerHandler
from CommonUtil import executeCommand

class PilotManagerComponent(Harness):
    """ 
    _PilotManagerComponent_ 

    
    """
    def __init__(self, config):
        """ 
        __init__ 
        """

        self.jobavaliableflag = False
        self.tarPath = config.PilotManagerComponent.tarPath
        self.pilotcodeDir = config.PilotManagerComponent.pilotCode 
        self.tqServer = config.PilotManagerComponent.tqAddress
        self.emulationMode = config.PilotManagerComponent.emulationMode
        self.pilotParams = None
        try:
            cfg = loadProdAgentConfiguration()
            pilot = cfg.getConfig("Pilot")
            self.pilotParams = pilot
        except:
            logging.debug("could not load PRODAGENT_CONFIG file")
        if ( not self.pilotParams ):
            self.pilotParams={'badAttempts':6,'noTaskAttempts':6}

        Harness.__init__(self, config)
	
    def preInitialization(self):
        """ 
        __preInitialization__ 
        """ 
    
        if not hasattr(self.config.PilotManagerComponent, "pilotManagerHandler"):
	    self.config.PilotManagerComponent.pilotManagerHandler = 'PilotManager.Handler.PilotManagerHandler'
	
	factory = WMFactory('PilotManager');
	self.messages["NewPilotJob"] = factory.loadObject(\
	                    self.config.PilotManagerComponent.pilotManagerHandler, self)
	self.messages["SubmitPilotJob"] = factory.loadObject(\
                            self.config.PilotManagerComponent.pilotManagerHandler, self)
        
        #create pilot tar file
        self.createPilotTar()

    def postInitialization(self):
        """ 
        __postInitialization__
        """
        pass

 
    def createPilotTar(self):
        """ 
        __createPiloTar__ 

        it will create the tar ball of pilot code
        """

        #change tar to tar.gz
        fileName = self.config.PilotManagerComponent.pilotTar
        tarCommand = "tar -czvf %s/%s -C %s Pilot" %(self.tarPath, fileName, self.pilotcodeDir)

        print tarCommand
        #myThread.logging.debug(tarCommand)
        output = executeCommand(tarCommand)
