#!/usr/bin/env python

"""
_PilotManagerClient_



"""

__revision__ = "$Id: PilotMonitorClient.py,v 1.3 2010/02/05 14:17:35 meloam Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "Khawar.Ahmad@cern.ch"

import os
import time
import inspect
import threading

#from MessageService.MessageService import MessageService
from WMCore.Agent.Configuration import loadConfigurationFile
from WMCore.WMFactory import WMFactory
from WMCore.Database.Transaction import Transaction
from WMCore.Database.DBFactory import DBFactory
#for logging
import logging
#TaskQueue
from PilotMonitor.PilotMonitorComponent import PilotMonitorComponent


config = loadConfigurationFile(\
                       '/data/khawar/prototype/PilotMonitor/DefaultConfig.py')

config.section_("General")

config.General.workDir = '/data/khawar/prototype/work/PilotMonitor'
config.Agent.componentName='PilotMonitor'

config.section_("CoreDatabase")
config.CoreDatabase.dialect = 'mysql'
config.CoreDatabase.socket = '/data/khawar/PAProd/0_12_13/prodAgent/mysqldata/mysql.sock'
config.CoreDatabase.user = 'root'
config.CoreDatabase.passwd = '98passwd'
config.CoreDatabase.hostname = 'localhost'
config.CoreDatabase.name = 'ProdAgentDB'


#start the module
pilotMonitor = PilotMonitorComponent(config)
pilotMonitor.prepareToStart()


#pilotManager.handleMessage('NewPilotJob','Pilot#45,45,xyzfiles')
msg={'site':'CERN', 'submissionMethod':'LSF'}
pilotMonitor.startDaemon()
#pilotMonitor.handleMessage('MonitorPilots', msg)

