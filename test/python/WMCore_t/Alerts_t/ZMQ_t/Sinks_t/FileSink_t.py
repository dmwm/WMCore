import os
import unittest
import time
import logging

from WMCore.Configuration import ConfigSection
from WMCore.Alerts.Alert import Alert
from WMCore.Alerts.ZMQ.Sinks.FileSink import FileSink
from WMQuality.TestInit import TestInit



class FileSinkTest(unittest.TestCase):
    def setUp(self):
        self.testInit = TestInit(__file__)
        self.testInit.setLogging(logLevel = logging.DEBUG)
        self.testDir = self.testInit.generateWorkDir()
        self.config = ConfigSection("file")
        self.config.outputfile = os.path.join(self.testDir, "FileSinkTestNew.json")


    def tearDown(self):
        self.testInit.delWorkDir()


    def testFileSinkBasic(self):
        sink = FileSink(self.config)
        alerts = []
        nAlerts = 10
        for i in range(nAlerts):
            a = Alert(Source = __file__, Level = i, Timestamp = time.time(),
                      Type = "Test")
            alerts.append(a)
        sink.send(alerts)
        # test by reading back
        loadAlerts = sink.load()
        self.assertEqual(len(loadAlerts), nAlerts)

        # Since FileSink implementation depends on line-separated JSONs of
        # Alert instance, test handling new lines in the payload
        alerts = []
        testMsg = "addtional \n message"
        for i in range(10, 20):
            a = Alert(Source = __file__, Level = i, Timestamp = time.time(),
                      Type = "Test", Details = {"message": testMsg})
            alerts.append(a)
        self.failUnless(os.path.exists(self.config.outputfile))
        sink.send(alerts)
        # test by reading back
        loadAlerts = sink.load()
        self.assertEqual(len(loadAlerts), 20)
        for a in loadAlerts[10:]:
            self.assertEqual(a["Details"]["message"], testMsg)



if __name__ == "__main__":
    unittest.main()
