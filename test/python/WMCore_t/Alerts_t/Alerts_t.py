#!/usr/bin/env python2.4
"""

"""

__revision__ = "$Id: Alerts_t.py,v 1.1 2008/10/22 21:34:35 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

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

class AlertsTest(unittest.TestCase):
    """
    _AlertsTest_
    
    """

    _setup = False
    _teardown = False
    # values for testing various sizes
    _triggers = 2
    _jobspecs = 5
    _flags = 4

    def setUp(self):
        """
        _setUp_
        
        """
        if AlertsTest._setup:
            return

        print("Alerts setup (once)")
        logging.basicConfig(level=logging.DEBUG,
                            format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
                            datefmt="%m-%d %H:%M",
                            filename="%s.log" % __file__,
                            filemode="w")

        myThread = threading.currentThread()
        myThread.logger = logging.getLogger("AlertsTest")
        myThread.dialect = os.getenv("DIALECT")
        
        options = {}
        if myThread.dialect == "MySQL":
            options["unix_socket"] = os.getenv("DBSOCK")
            dbFactory = DBFactory(myThread.logger, os.getenv("DATABASE"), \
                                  options)
        else:
            dbFactory = DBFactory(myThread.logger, os.getenv("DATABASE"))
        
        myThread.dbi = dbFactory.connect() 
        
        factory = WMFactory("alerts", "WMCore.Alerts")
        create = factory.loadObject(myThread.dialect+".Create")
        destroy = factory.loadObject(myThread.dialect+".Destroy")        
        myThread.transaction = Transaction(myThread.dbi)
        destroy.execute(conn = myThread.transaction.conn)
        createworked = create.execute(conn = myThread.transaction.conn)
        if not createworked:
            raise Exception("Alert tables could not be created, already exist?")
        AlertsTest._setup = True
        myThread.transaction.commit()                                  

    def tearDown(self):
        """
        Database deletion 
        """
        pass
    
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
        pass

    def runTest(self):
        self.testPublish() 

if __name__ == "__main__":
    unittest.main()
