import os
import unittest
import time
from WMCore.Configuration import ConfigSection
from WMCore.Alerts.Alert import Alert
from WMCore.Alerts.ZMQ.Sinks.FileSink import FileSink



class FileSinkTest(unittest.TestCase):
    def setUp(self):
        self.config = ConfigSection("file")
        self.config.outputfile = "/tmp/FileSinkTestNew.json"
    
    
    def tearDown(self):
        if os.path.exists(self.config.outputfile):
            os.remove(self.config.outputfile)
    
    
    def testFileSinkBasic(self):
        sink = FileSink(self.config)
        alerts = []
        for i in range(10):
            a = Alert(Source = __file__, Level = i, Timestamp = time.time(),
                      Type = "Test")
            alerts.append(a)
        sink.send(alerts)
        self.failUnless(os.path.exists(self.config.outputfile))
        # test by reading back
        loadAlerts = sink.load()
        self.assertEqual(len(loadAlerts), 10)
        
        # Since FileSink implementation depends on line-separated JSONs of
        # Alert instance, test handling new lines in the payload
        alerts = []
        testMsg = "addtional \n message"
        for i in range(10, 20):
            a = Alert(Source = __file__, Level = i, Timestamp = time.time(),
                      Type = "Test", Details = {"message": testMsg})
            alerts.append(a)
        sink.send(alerts)
        self.failUnless(os.path.exists(self.config.outputfile))
        # test by reading back
        loadAlerts = sink.load()
        self.assertEqual(len(loadAlerts), 20)
        for a in loadAlerts[10:]:
            self.assertEqual(a["Details"]["message"], testMsg)



if __name__ == "__main__":
    unittest.main()