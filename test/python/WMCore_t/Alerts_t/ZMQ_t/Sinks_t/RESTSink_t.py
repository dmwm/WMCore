import time
import unittest

from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore.Configuration import ConfigSection
from WMCore.Alerts.Alert import Alert
from WMCore.Alerts.ZMQ.Sinks.RESTSink import RESTSink



class RESTSinkTest(unittest.TestCase):
    def setUp(self):
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        dbName = "alerts-rest_sink"
        self.testInit.setupCouch(dbName)

        self.config = ConfigSection("rest")
        self.config.uri = self.testInit.couchUrl + "/" + dbName


    def tearDown(self):
        self.testInit.tearDownCouch()


    def testRESTSinkBasic(self):
        sink = RESTSink(self.config)
        docIds = []
        alerts = []
        for i in range(10):
            a = Alert(Source = __file__, Level = i, Timestamp = time.time(),
                      Type = "Test")
            alerts.append(a)
        retVal = sink.send(alerts)

        # return value is following format:
        # [{'rev': '1-ba0a0903d4d6ddcbb85ff64d48d8be14', 'id': 'b7e8f807c96f572418b39422ccea252c'}]
        # just 1 item was added in the list of alerts, so retVal is also 1 item list
        # and CMSCouch call commitOne also returns a list - hence second nesting
        changes = sink._database.changes()
        self.assertEqual(len(changes[u"results"]), 10)
        self.assertEqual(changes[u"last_seq"], 10)

        alerts = []
        for i in range(10, 20):
            a = Alert(Source = __file__, Level = i, Timestamp = time.time(),
                      Type = "Test")
            alerts.append(a)
        retVals = sink.send(alerts)

        changes = sink._database.changes()
        self.assertEqual(len(changes[u"results"]), 10)
        self.assertEqual(changes[u"last_seq"], 20)


if __name__ == "__main__":
    unittest.main()
