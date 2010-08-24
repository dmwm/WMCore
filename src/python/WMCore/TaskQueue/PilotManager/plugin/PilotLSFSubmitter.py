#!/usr/bin/env python

"""
_PilotLSFSubmitter_

"""

from ProdAgent.Resources.LSF import LSFConfiguration
from PilotManager.CommonUtil import executeCommand
#from JobSubmitter.Submitters.BulkSubmitterInterface import makeNonBlocking
from JobSubmitter.JSException import JSException
from PilotManager.plugin.Registry import registerManager
from PilotManager.plugin.LSFInterface import loadLSFConfig

import datetime
import logging
import sys
import os

class PilotLSFSubmitter:

    def __init__(self):
        self.schedType = 'LSF' 

    def __call__(self, schedType):
        pass

    def submitPilot(self, taskName, exe, exePath, inputSandbox, bulkSize):

        shellScript = exe
        scriptPath = exePath
        lsfConfig = loadLSFConfig()
        if ( lsfConfig == None ):
            logging.debug('lsfConfig is none. returing back the call')
            return

    	#start lsf submission command
        lsfSubmitCommand = 'bsub'
        lsfQueue = lsfConfig['Queue']
	lsfSubmitCommand += ' -q %s -J %s' % (lsfQueue, taskName)
	
	#creating the log directory.
	lsfLogDir = lsfConfig['logDir']
        #'/afs/cern.ch/user/k/khawar/scratch2/khawar/logs'
	if ( lsfLogDir != 'None' ):
            now = datetime.datetime.today()
	    lsfLogDir += '/%s' % now.strftime("%Y%m%d%H")
	    try:
	        os.mkdir(lsfLogDir)
	        logging.debug("Created directory %s" % lsfLogDir)
	    except OSError, err:
	        # suppress LSF log unless it's about an already exisiting directory
	        if err.errno != errno.EEXIST or not os.path.isdir(lsfLogDir):
	            logging.debug("Can't create directory %s, turning off LSF log" % lsfLogDir)
	            lsfLogDir = 'None'
	
	lsfSubmitCommand += ' -g %s' % LSFConfiguration.getGroup()
	if ( lsfLogDir == "None" ):
	    lsfSubmitCommand += ' -oo /dev/null'
	else:
	    lsfSubmitCommand += ' -oo %s/%s.lsf.log' % (lsfLogDir,'pilot')
	
	lsfSubmitCommand += ' < %s' % os.path.join(scriptPath, shellScript)
        
	failureList = []
	try:
            output = executeCommand(lsfSubmitCommand)
            logging.info("PilotManager.submitPilotJob: %s " % output)
	    logging.info("PilotManager.submitPilotJob: %s " %lsfSubmitCommand )
        except RuntimeError, err:
            failureList.append('jobSpec')

        if len(failureList) > 0:
            raise JSException("Submission Failed", FailureList = failureList)

registerManager(PilotLSFSubmitter, PilotLSFSubmitter.__name__)
