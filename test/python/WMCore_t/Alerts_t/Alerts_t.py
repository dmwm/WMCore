#!/usr/bin/env python2.4
"""

"""

__revision__ = "$Id: Alerts_t.py,v 1.6 2009/10/13 22:30:58 meloam Exp $"
__version__ = "$Revision: 1.6 $"

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

    # values for testing various sizes
    _triggers = 2
    _jobspecs = 5
    _flags = 4

    def setUp(self):
        """
        _setUp_
        

        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ['WMCore.Alerts'], useDefault = False)
            

            
    def tearDown(self):
        """
        Database deletion 
        """
        self.testInit.clearDatabase()
            
    
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

        #print "Publishing Alerts:"
        publishedAlerts = []
        for i in range(5):
            alertComponent = random.choice(components)            
            alertSeverity = random.choice(severities)
            alertMessage = random.choice(messages)

            alert = "  %s: %s - %s" % (alertSeverity, alertMessage,
                                       alertComponent)
            publishedAlerts.append(alert)
            #print alert            

            alertSystem.publishAlert(alertSeverity, alertComponent,
                                     alertMessage)

        #print "Retrieving Alerts:"
        currentAlerts = alertSystem.listCurrentAlerts()

        if len(publishedAlerts) != len(currentAlerts):
            return False
        
        for currentAlert in currentAlerts:
            formattedAlert = "  %s: %s - %s" % (currentAlert["severity"],
                                                currentAlert["message"],
                                                currentAlert["component"])
            #    print formattedAlert

            if formattedAlert not in publishedAlerts:
                return False

        return


if __name__ == "__main__":
    unittest.main()
