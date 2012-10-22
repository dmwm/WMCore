import unittest
import time

from WMCore.Alerts.Alert import Alert
from WMCore.Alerts.Alert import RegisterMsg, UnregisterMsg, ShutdownMsg



class AlertTest(unittest.TestCase):
    def setUp(self):
        pass


    def tearDown(self):
        pass


    def testAlertBasic(self):
        a = Alert()
        self.assertEqual(a.level, 0)
        self.assertEqual(a["Source"], None)
        self.assertEqual(a["Type"], None)
        self.assertEqual(a["Workload"], None)
        self.assertEqual(a["Component"], None)
        self.assertEqual(a["Details"], {})
        self.assertEqual(a["Timestamp"], None)
        self.assertEqual(a["TimestampDecoded"], None)

        details = dict(detail = "detail")
        a = Alert(Level = 5, Source = "src", Type = "type", Workload = "work",
                  Component = "comp", Details = details, Timestamp = "time")
        self.assertEqual(a.level, 5)
        self.assertEqual(a["Source"], "src")
        self.assertEqual(a["Type"], "type")
        self.assertEqual(a["Workload"], "work")
        self.assertEqual(a["Component"], "comp")
        self.assertEqual(a["Details"], details)
        self.assertEqual(a["Timestamp"], "time")
        a.toMsg()


    def testSetTimestamp(self):
        a = Alert()
        self.assertEqual(a["Timestamp"], None)
        self.assertEqual(a["TimestampDecoded"], None)
        a.setTimestamp()
        self.assertTrue(isinstance(a["Timestamp"], float))
        tsd = a["TimestampDecoded"]
        tsdTested = time.strftime(a.TIMESTAMP_FORMAT, time.gmtime(a["Timestamp"]))
        self.assertEqual(tsd, tsdTested)


    def testRegisterMsg(self):
        msg = RegisterMsg("mylabel")
        self.assertEqual(msg.key, "Register")
        self.assertEqual(msg[msg.key], "mylabel")


    def testUnregisterMsg(self):
        msg = UnregisterMsg("anotherlabel")
        self.assertEqual(msg.key, "Unregister")
        self.assertEqual(msg[msg.key], "anotherlabel")


    def testShutdownMsg(self):
        msg = ShutdownMsg()
        self.assertEqual(msg.key, "Shutdown")
        self.assertEqual(msg[msg.key], True)



if __name__ == "__main__":
    unittest.main()
