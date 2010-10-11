"""
Use this for only unit test
"""
import os
import tempfile
import sys

from WMCore.Configuration import Configuration, saveConfigurationFile
    
def emulatorSetup(phedex=False, dbs=False, siteDB=False, requestMgr=False):
    fd,configFile = tempfile.mkstemp(".py", "Emulator_Config",)
    os.environ["EMULATOR_CONFIG"] = configFile
    _emulatorConfig(phedex, dbs, siteDB, requestMgr, configFile)
    return configFile
    
def deleteConfig(configFile):
    os.remove(configFile)
        
def _emulatorConfig(phedex, dbs, siteDB, requestMgr, configFile):
    
    config = Configuration()
    config.section_("Emulator")
    config.Emulator.PhEDEx = phedex
    config.Emulator.DBSReader = dbs
    config.Emulator.RequestMgr = requestMgr
    config.Emulator.SiteDB = siteDB
    saveConfigurationFile(config, configFile)
    print "create config file:%s, PhEDEx: %s, DBS: %s, RequestManager: %s, SiteDB %s with flag" \
           % (configFile, phedex, dbs, siteDB, requestMgr)
           
def setupWMAgentConfig():
    fd,configFile = tempfile.mkstemp(".py", "TESTAGENTConfig",)
    os.environ["WMAGENT_CONFIG"] = configFile
    _wmAgentConfig(configFile)
    return configFile

def _wmAgentConfig(configFile):

    config = Configuration()
    config.section_("JobStateMachine")
    #Waring setting couchDB to None will cause the ERROR:
    # but that should be ignored, if you want to test couchDB
    # set the real couchDB information here
    config.JobStateMachine.couchurl = os.getenv("COUCHURL")
    config.JobStateMachine.couchDBName = os.getenv("COUCHDB")
    saveConfigurationFile(config, configFile)