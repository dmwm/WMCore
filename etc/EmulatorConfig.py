'''
Created on Feb 15, 2010

'''
from WMCore.Configuration import Configuration

config = Configuration()
config.section_("Emulator")
config.Emulator.PhEDEx = True
config.Emulator.DBSReader = True
config.Emulator.ReqMgr = True
