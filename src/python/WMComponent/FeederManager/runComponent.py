#!/usr/bin/env python

import os
import time
import threading
import pickle

from WMCore.Agent.Configuration import loadConfigurationFile
from WMComponent.FeederManager.FeederManager import FeederManager

path = os.path.join(os.getenv("WMCOREBASE"),
                    "src/python/WMComponent/FeederManager/DefaultConfig.py")
print path
config = loadConfigurationFile(path)
config.Agent.contact = "james.jackson@cern.ch"
config.Agent.teamName = "JamesTesting"
config.Agent.agentName = "James's agent"
config.Agent.componentName = "FeederManager"

config.section_("General")
config.General.workDir = os.getenv("TESTDIR")

config.section_("CoreDatabase")
config.CoreDatabase.dialect = "mysql"
config.CoreDatabase.socket = os.getenv("DBSOCK")
config.CoreDatabase.user = os.getenv("DBUSER")
config.CoreDatabase.passwd = os.getenv("DBPASS") 
config.CoreDatabase.hostname = os.getenv("DBHOST")
config.CoreDatabase.name = os.getenv("DBNAME")

# Start the component
testFeederManager = FeederManager(config)
print "Starting"
testFeederManager.prepareToStart()
print "Started"

# Prepare message
message = {"FeederType" : "runTransferNotifier", "DataSetName" : "a name",\
           "RequireParents" : False, "CallbackId" : "0"}

testFeederManager.handleMessage("AddDatasetWatch", pickle.dumps(message))

message = {"FeederType" : "otherType", "DataSetName" : "a name",\
           "RequireParents" : False, "CallbackId" : "0"}

testFeederManager.handleMessage("AddDatasetWatch", pickle.dumps(message))

message = {"FeederType" : "runTransferNotifier", "DataSetName" : "a name",\
           "RequireParents" : False, "CallbackId" : "0"}

testFeederManager.handleMessage("AddDatasetWatch", pickle.dumps(message))

# Wait a bit
time.sleep(5)

# Shutdown the component
testFeederManager.prepareToStop()
