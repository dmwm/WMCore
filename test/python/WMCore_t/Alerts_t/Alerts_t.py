#!/usr/bin/env python2.4
"""

"""

__revision__ = "$Id: Alerts_t.py,v 1.3 2009/07/10 21:46:06 sryu Exp $"
__version__ = "$Revision: 1.3 $"

import commands
import unittest
import logging
import os
import threading
import random

from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction
from WMCore.Alerts.Alerts import Alerts
from WMCore.WMFactory import WMFactory

from WMQuality.TestInit import TestInit

class AlertsTest(unittest.TestCase):
    """
    _AlertsTest_
    
    """

    _setup = True
    _teardown = True
    # values for testing various sizes
    _triggers = 2
    _jobspecs = 5
    _flags = 4

    def setUp(self):
        """
        _setUp_
        

        """
        if AlertsTest._setup:
            self.testInit = TestInit(__file__, os.getenv("DIALECT"))
            self.testInit.setLogging()
            self.testInit.setDatabaseConnection()
            self.testInit.setSchema(customModules = ['WMCore.Alerts'], useDefault = False)
            
            #AlertTest._setup = False
            
    def tearDown(self):
        """
        Database deletion 
        """
        myThread = threading.currentThread()
        if AlertsTest._teardown :
            # call the script we use for cleaning:
            self.testInit.clearDatabase(modules = ["WMCore.Alerts"])
            #AlertsTest._teardown = False
            
    
    def testPublish(self):
        """
        __testPublish__

        Test publishing alerts.
        """
        myThread = threading.currentThread()
        alertSystem = Alerts()

        components = ["Tier0Flusher", "ErrorHandler", "SomeOtherComponent"]
        severities = ["Critical", "Severe", "Minor", "Notice"]
        messages = ["SERVER ON FIRE", "Your burrito just exploded.",
                    "The dollar is worthless."]

        print "Publishing Alerts:"
        publishedAlerts = []
        for i in range(5):
            alertComponent = random.choice(components)            
            alertSeverity = random.choice(severities)
            alertMessage = random.choice(messages)

            alert = "  %s: %s - %s" % (alertSeverity, alertMessage,
                                       alertComponent)
            publishedAlerts.append(alert)
            print alert            

            alertSystem.publishAlert(alertSeverity, alertComponent,
                                     alertMessage)

        print "Retrieving Alerts:"
        currentAlerts = alertSystem.listCurrentAlerts()

        if len(publishedAlerts) != len(currentAlerts):
            return False
        
        for currentAlert in currentAlerts:
            formattedAlert = "  %s: %s - %s" % (currentAlert["severity"],
                                                currentAlert["message"],
                                                currentAlert["component"])
            print formattedAlert

            if formattedAlert not in publishedAlerts:
                return False

        return
            
    def testAckCurrent(self):
        """
        _testAckCurrent_
        
        """
        AlertsTest._teardown = True

    def runTest(self):
        self.testPublish() 
        self.testAckCurrent()

if __name__ == "__main__":
    unittest.main()
