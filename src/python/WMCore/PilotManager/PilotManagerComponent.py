#!/usr/bin/env python

"""
_JobQueueComponent_



"""





import os
import time
import inspect
import threading

#from MessageService.MessageService import MessageService
from WMCore.Agent.Harness import Harness
# loaded from the config file.
from WMCore.WMFactory import WMFactory

#for logging
import logging
import ProdAgentCore.LoggingUtils as LoggingUtils
from Handler.PilotManagerHandler import PilotManagerHandler
from CommonUtil import executeCommand

class PilotManagerComponent(Harness):
    """ 
    _PilotManagerComponent_ 

    
    """
    def __init__(self, config):
        self.jobs = {}
        self.jobavaliableflag = False
        self.tarPath = config.PilotManagerComponent.tarPath
        self.pilotcodeDir = config.PilotManagerComponent.pilotCode 
        self.tqServer = config.PilotManagerComponent.tqAddress
        self.emulationMode = config.PilotManagerComponent.emulationMode

        Harness.__init__(self, config)
        #print (config)
	
    def preInitialization(self):
    
        if not hasattr(self.config.PilotManagerComponent, "pilotManagerHandler"):
	    self.config.PilotManagerComponent.pilotManagerHandler = 'PilotManager.Handler.PilotManagerHandler'
	
	factory = WMFactory('PilotManager');
	self.messages["NewPilotJob"] = factory.loadObject(\
	                    self.config.PilotManagerComponent.pilotManagerHandler, self)
	self.messages["SubmitPilotJob"] = factory.loadObject(\
                            self.config.PilotManagerComponent.pilotManagerHandler, self)
        
        #create pilot tar file
        #self.createPilotTar()

    def postInitialization(self):
        """ __postInitialization__ """
        print 'postInitialization'
        self.createPilotTar()
 
    def createPilotTar(self):
        """ __createPiloTar__ """
        tarCommand = "tar -cvf %s/Pilot.tar -C %s Pilot" %(self.tarPath, self.pilotcodeDir)
        #myThread.logging.debug(tarCommand)
        output = executeCommand(tarCommand)
        print output 
         			    
