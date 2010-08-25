#!/usr/bin/env python

"""
_PilotManagerComponent_



"""

__revision__ = "$Id: PilotManagerComponent.py,v 1.1 2009/07/30 22:29:11 khawar Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Khawar.Ahmad@cern.ch"

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
#from Handler.PilotManagerHandler import PilotManagerHandler
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
        """ __createPiloTar__ """
        tarCommand = "tar -cvf %s/Pilot.tar -C %s Pilot" %(self.tarPath, self.pilotcodeDir)
        print tarCommand
        #myThread.logging.debug(tarCommand)
        output = executeCommand(tarCommand)
        #print output 

