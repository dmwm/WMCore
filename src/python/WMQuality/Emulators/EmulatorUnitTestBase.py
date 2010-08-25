import unittest
import os
import tempfile
import sys

from WMCore.Configuration import Configuration, saveConfigurationFile

class EmulatorUnitTestBase(unittest.TestCase):
    
    def setUp(self):
        fd, self.configFile = tempfile.mkstemp(".py", "Emulator_Config",)
        os.environ["EMULATOR_CONFIG"] = self.configFile
        self.setEmulator()
        self._emulatorCofig()
        
    def tearDown(self):
        os.remove(self.configFile)
        print "file deleted: %s" % self.configFile
        
    def _emulatorCofig(self):
        
        config = Configuration()
        config.section_("Emulator")
        config.Emulator.PhEDEx = self.phedexFlag
        config.Emulator.DBSReader = self.dbsFlag
        config.Emulator.RequestMgr = self.requestMgrFlag
        config.Emulator.SiteDB = self.siteDBFlag
        saveConfigurationFile(config, self.configFile)
        print "create config file:%s, PhEDEx: %s, DBS: %s, RequestManager: %s, with flag" \
               % (self.configFile, self.phedexFlag, self.dbsFlag, self.requestMgrFlag)
               
    def setEmulator(self):
        """
        overwrite this in the child class if diffent setting is needed.
        """
        self.phedexFlag = True
        self.dbsFlag = True
        self.requestMgrFlag = True
        self.siteDBFlag = True