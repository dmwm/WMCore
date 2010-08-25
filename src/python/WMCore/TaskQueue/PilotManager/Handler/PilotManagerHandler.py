#!/usr/bin/env python
"""
_PilotManagerHandler_

Handler for SubmitPilotJob  
"""

from WMCore.Agent.BaseHandler import BaseHandler

import threading
#import logging
import cPickle
import simplejson
import base64
import random
import socket
import datetime
import stat
import os
import sys
import errno

from WMCore.Agent.BaseHandler import BaseHandler
from ProdAgent.Resources.LSF import LSFConfiguration
#from JobSubmitter.Submitters.BulkSubmitterInterface import makeNonBlocking
#from JobSubmitter.JSException import JSException
from PilotManager.CommonUtil import executeCommand
#from PilotManager.plugin.PilotLSFSubmitter import  PilotLSFSubmitter
#from PilotManager.plugin.PilotBossSubmitter import PilotBossSubmitter
from PilotManager.plugin.Registry import retrieveManager

class PilotManagerHandler(BaseHandler):

    def __init__(self, component):
        #super class init
        self.jobs = {}
        self.jobavaliableflag = False
        BaseHandler.__init__(self, component)
        myThread = threading.currentThread()
        self.logger = myThread.logger

    def __call__(self, event, payload):
        self.logger.debug("PilotManagerHandler:__call__:event:" + str( event ) ) 
        print payload
        if event == "NewPilotJob":
            self.sendNewPilotJob( payload )
        elif event == "SubmitPilotJob":
            self.submitPilotJob( payload )

    def createPilotConfig(self):
        """ 
	__createPilotConfig__
	
	create configuration for pilot instance
	"""
	randNo = random.randint(1,10000)
	
	pilotConfig = {'pilotID': randNo, 'pilotName': 'Pilot_%s' % randNo, \
	               'serverMode': False, 'serverPort': 10, \
		       'tqaddress': \
                       self.component.config.PilotManagerComponent.tqAddress, \
                       'TTL': -1 }
	
	tmp = cPickle.dumps( pilotConfig )
        encodedConfig = base64.encodestring(tmp);
        return encodedConfig
    
    ################################################
    # generate script required to launch pilot code
    ################################################
    def makePilotJobScript( self, pilotConfig, hostname, pilotTarPath, pilotTar  ):
        """ 
	__makePilotJobScript__

	create executable script for this pilot job
	"""
	lines = '#PilotJob startup script \n'
	lines += "PILOT_DIR=\"`pwd`\" \n"
	lines += "PILOT_CONFIG=\"%s\" \n" % pilotConfig 
	lines += "echo $PILOT_DIR \n"
	lines += "echo $PILOT_CONFIG \n"
	#lines += "rfcp %s:%s . \n" % (hostname, pilotTarPath)
	lines += "tar -xf $PILOT_DIR/%s > /dev/null 2>&1\n" % pilotTar
	lines += "cd Pilot \n"
	lines += "#( /usr/bin/time ./pilotrun.sh $PILOT_CONFIG 2>&1 ) | gzip > ./pilotrun.log.gz\n"
	lines += "python PilotClient.py --pconfig=\"$PILOT_CONFIG\" \n"
	lines += "echo 'hello world' \n"
	lines += "echo $PILOT_CONFIG \n"

	return lines	
    
    ################################
    # save script to given file name
    ################################
    def save(self, filename, script ):
        """ 
	__save__ 

	save pilot job executable script
	"""
	handle = open(filename, 'w')
        handle.write(script)
        handle.close()
        #apply all permissions
        os.chmod(filename, stat.S_IRWXU|stat.S_IRWXG|stat.S_IRWXO)

    ########################################
    #generate pilot config params and submit
    ########################################			
    def submitPilotJob( self, payload ):
        """ 
        __submitPilotJob__
        """
        self.logger.debug("PilotManagerHandler:submitPilotJob : %s" % payload)
        payload = cPickle.loads(payload['payload'])
        site = payload['site'] 
        bulkSize = payload['bulkSize']
        submissionMethod = payload['submissionMethod']

        if ( bulkSize == 0):
            self.logger.debug( 'no jobs to submit')
            return  	

        #encoded configuration passed to pilot
	pilotConfig = self.createPilotConfig()
	
	#hostname
        hostname = socket.getfqdn() 
	 
	#or could be created dynamically
	pilotTar = self.component.config.PilotManagerComponent.pilotTar 

        #path where Pilot.tar is located	
        pilotTarFilePath = "%s/Pilot.tar" % \
                       self.component.config.PilotManagerComponent.tarPath
	
	#remove it if pilots script is same for everyone
	pilotScript = self.makePilotJobScript( pilotConfig, hostname,\
                       pilotTarFilePath, pilotTar )
	
	#save this pilot script
	pilotfilename = self.component.config.PilotManagerComponent.componentDir+"/pilotsubmit.sh"
	
	#save this pilot bootstrap script
        #no need to save if no dyn param
        self.save( pilotfilename, pilotScript )
	
	#Scheduler information: get it from PJManager config or from msg payload
	#for default: self.component.config.PilotManagerComponent.defaultScheduler
        try:
            self.logger.debug("Using plugin: %s" % \
            self.component.config.PilotManagerComponent.plugin )
            submitter = retrieveManager(\
            self.component.config.PilotManagerComponent.plugin)
            #PilotBossSubmitter(submissionMethod)
            submitter(submissionMethod)
        except:
            self.logger.debug("Could not retrieve : %s " % \
            self.component.config.PilotManagerComponent.plugin )

        try:
            self.logger.debug( pilotfilename)
            self.logger.debug( pilotTarFilePath )
	    submitter.submitPilot('PilotJob', 'pilotsubmit.sh', \
	                     self.component.config.PilotManagerComponent.componentDir, \
		   	     pilotTarFilePath, bulkSize ) 
        except:
            self.logger.debug( 'PilotManager: Submission Error %s:%s' % \
                   (sys.exc_info()[0], sys.exc_info()[1]) )
 
    ###########################################
    # method is used to send pilot to emulator
    ###########################################	
    def sendNewPilotJob( self, payload ):
        
        self.logger.debug("PilotManagerHandler:sendNewPilotJob:payload:" + str( payload ) )
        #for testing
        randNo = random.randint(1,10000)
        #now build the pilot configurations
        #it must be policy dependent
        pilotConfig = {'pilotID':randNo, 'pilotName':'Pilot_%s'%randNo, \
	               'serverMode':False, 'serverPort':10, \
                       'tqaddress':'vocms13.cern.ch:8030', 'TTL':-1 }
	
        payload = {'pilotConfig':pilotConfig}
        tmp =  cPickle.dumps( payload )
        encodedConfig = base64.b64encode(tmp) 

        #construct message
        if ( self.component.emulationMode ):
            self.logger.debug("generating a new pilot job for emulation")

            msg ={ 'name': 'EmulatePilotJob', \
                   'payload': encodedConfig, \
                   'instant': True }
            #now send message
            self.sendServiceMsg(msg)

    ###################################
    #method to publish messages
    ###################################
    def sendServiceMsg(self, msg):
        self.logger.debug("sending message from PilotManagerHandler")
        myThread = threading.currentThread()
        myThread.transaction.begin()
        myThread.msgService.publish(msg)
        myThread.transaction.commit()
