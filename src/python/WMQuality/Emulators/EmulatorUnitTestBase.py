import unittest
import sys

class EmulatorUnitTestBase(unittest.TestCase):
    
    reloaded = False
    emulatorModules = {
         "PhEDEx" : ("WMCore.Services.PhEDEx.PhEDEx",
                     "WMQuality.Emulators.PhEDExClient.PhEDEx"),
         "DBS"    : ("WMCore.Services.DBS.DBSReader",
                     "WMQuality.Emulators.DBSClient.DBSReader"),
         "RequestManager" : ("WMCore.Services.RequestManager.RequestManager",
                              "WMQuality.Emulators.RequestManagerClient.RequestManager"),
         "SiteDB" : ("WMCore.Services.SiteDB.SiteDB",
                     "WMQuality.Emulators.SiteDBClient.SiteDB")
         }
    
    def setUp(self):
        if not EmulatorUnitTestBase.reloaded:
            self.setEmulator()
            self._reloadModule("PhEDEx", self.phedexFlag)
            self._reloadModule("DBS", self.dbsFlag)
            self._reloadModule("RequestManager", self.requestMgrFlag)
            self._reloadModule("SiteDB", self.siteDBFlag)
            EmulatorUnitTestBase.reloaded = True
            
    def _reloadModule(self, key, flag):
        
        if flag:
            i = 1
        else:
            i = 0
        
        try:
            reload(sys.modules[EmulatorUnitTestBase.emulatorModules[key][i]])
        except KeyError:
            __import__(EmulatorUnitTestBase.emulatorModules[key][i])
            
    def setEmulator(self):
        """
        overwrite this in the child class if diffent setting is needed.
        """
        self.phedexFlag = True
        self.dbsFlag = True
        self.requestMgrFlag = True
        self.siteDBFlag = True
