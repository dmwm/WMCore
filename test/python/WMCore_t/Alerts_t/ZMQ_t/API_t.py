"""
API_t

test of the client's API for Alerts framework.

"""


import time
import unittest
import threading

from WMCore.WorkerThreads.BaseWorkerThread  import BaseWorkerThread
from WMCore.Configuration import Configuration
from WMCore.Alerts import API as alertAPI

from WMQuality.TestInit import TestInit
from WMComponent_t.AlertGenerator_t.AlertGenerator_t import getConfig
from WMComponent_t.AlertGenerator_t.Pollers_t import utils



class APITest(unittest.TestCase):
    def setUp(self):
        """
        This much stuff for simple alerts client API testing is
        needed because it's also testing setting up alert fw as
        done in the BaseWorkerThread.
        
        """
        myThread = threading.currentThread()        
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testDir = self.testInit.generateWorkDir()
        # needed for instantiating BaseWorkerThread
        self.testInit.setDatabaseConnection()
        self.alertsReceiver = None


    def tearDown(self):
        self.testInit.clearDatabase()        
        self.testInit.delWorkDir()
        if self.alertsReceiver:
            self.alertsReceiver.shutdown()
            
                
    def testAlertsMessagingBasic(self):
        config = getConfig("/tmp")
        self.assertTrue(hasattr(config, "Alert"))        
        # initialization
        # sender: instance of Alert messages Sender
        # preAlert: pre-defined values for Alert instances generated from this class
        self.config = config # needed in setUpAlertsMessaging
        preAlert, sender = alertAPI.setUpAlertsMessaging(self,
                                                         compName = "testBasic")
        sendAlert = alertAPI.getSendAlert(sender = sender,
                                          preAlert = preAlert)

        # set up a temporary alert message receiver        
        handler, receiver = utils.setUpReceiver(config.Alert.address,
                                                config.Alert.controlAddr)
        # test sending alert
        msg = "this is my message Basic"
        sendAlert(100, msg = msg)
        
        time.sleep(0.5)
        self.assertEqual(len(handler.queue), 1)
        alert = handler.queue[0]
        self.assertEqual(alert["Component"], "testBasic")
        self.assertEqual(alert["Level"], 100)
        self.assertEqual(alert["Source"], self.__class__.__name__)
        self.assertEqual(alert["Details"]["msg"], msg) 
        
        sender.unregister()
        receiver.shutdown()
        

    def testAlertsMessagingNotSetUpViaBaseWorkerThread(self):
        # alerts will not be set up if 'config.Alert' etc is not provided
        config = Configuration()
        self.assertFalse(hasattr(config, "Alert"))
        # test the same way alerts are set up in the client code (currently
        # all cases via BaseWorkerThread)
        # this call creates .sender, but here will be set to None
        thread = BaseWorkerThread()
        thread.config = config
        thread.initAlerts(compName = "test1")
        self.assertFalse(thread.sender)
        # shall do nothing and not fail
        thread.sendAlert("nonsense", msg = "nonsense")
        
        
    def testAlertsSetUpAndSendingViaBaseWorkerThread(self):
        # calls as they are made from child/client classes of BaseWorkerThread
        config = getConfig("/tmp")
        self.assertTrue(hasattr(config, "Alert"))
        # test the same way alerts are set up in the client code (currently
        # all cases via BaseWorkerThread)
        # this call creates .sender, but here will be set to None
        thread = BaseWorkerThread()
        thread.config = config
        thread.initAlerts(compName = "test2")
        self.assertTrue(thread.sender)
        
        # set up a temporary alert message receiver        
        handler, receiver = utils.setUpReceiver(config.Alert.address,
                                                config.Alert.controlAddr)

        # send an alert message
        msg = "this is my message 1"
        thread.sendAlert(10, msg = msg)

        # test
        time.sleep(0.5)
        self.assertEqual(len(handler.queue), 1)
        alert = handler.queue[0]
        self.assertEqual(alert["Component"], "test2")
        self.assertEqual(alert["Level"], 10)
        self.assertEqual(alert["Source"], thread.__class__.__name__)
        self.assertEqual(alert["Details"]["msg"], msg) 
        
        thread.sender.unregister()
        receiver.shutdown()


        
if __name__ == "__main__":
    unittest.main()