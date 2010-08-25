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
    _emulatorCofig(phedex, dbs, siteDB, requestMgr, configFile)
    return configFile
    
def deleteConfig(configFile):
    os.remove(configFile)
    print "file deleted: %s" % configFile
        
def _emulatorCofig(phedex, dbs, siteDB, requestMgr, configFile):
    
    config = Configuration()
    config.section_("Emulator")
    config.Emulator.PhEDEx = phedex
    config.Emulator.DBSReader = dbs
    config.Emulator.RequestMgr = requestMgr
    config.Emulator.SiteDB = siteDB
    saveConfigurationFile(config, configFile)
    print "create config file:%s, PhEDEx: %s, DBS: %s, RequestManager: %s, SiteDB %s with flag" \
           % (configFile, phedex, dbs, siteDB, requestMgr)
           
    