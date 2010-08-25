#!/usr/bin/env python
#pylint: disable-msg=E1101,E1103,C0103,R0902

"""
Performs bulk DBS File(s) insertion by :
	reading the FJR received in payload
	buffering in the database
	if buffer has hit the configured limit
"""

__revision__ = "$Id: DBSUpload.py,v 1.5 2009/08/12 17:54:09 meloam Exp $"
__version__ = "$Revision: 1.5 $"
__author__ = "anzar@fnal.gov"

import logging
import threading

# harness class that encapsulates the basic component logic.
from WMCore.Agent.Harness import Harness
# we do not import failure handlers as they are dynamicly 
# loaded from the config file.
from WMCore.WMFactory import WMFactory

from WMComponent.DBSUpload.DBSUploadPoller import DBSUploadPoller




#['__call__', '__doc__', '__init__', '__module__', '__str__', 'config', 'handleMessage', 'initInThread', 'initialization', 'logState', 'postInitialization', 'preInitialization', 'prepareToStart', 'publishItem', 'startComponent']

class DBSUpload(Harness):

    def __init__(self, config):
		# call the base class
		Harness.__init__(self, config)
		self.pollTime = 1
		print "DBSUpload.__init__"

    def preInitialization(self):
		print "DBSUpload.preInitialization"
		
		# use a factory to dynamically load handlers.
		factory = WMFactory('generic')
		
		
		# Add event loop to worker manager
		myThread = threading.currentThread()
		
		pollInterval = self.config.DBSUpload.pollInterval
		logging.info("Setting poll interval to %s seconds" % pollInterval)
		myThread.workerThreadManager.addWorker(DBSUploadPoller(self.config), pollInterval)
		
		return










