#!/usr/bin/env python
"""
_Alerts_

WMCore alert system.
"""




from WMCore.DataStructs.Alert import Alert
from WMCore.WMFactory import WMFactory
from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction
from WMCore.WMException import WMException

import threading

class Alerts:
    """
    _Alerts_

    WMCore alert system.
    """
    def __init__(self):
        """
        ___init___

        Initialize the DAO factory
        """
        myThread = threading.currentThread()
        
        if myThread.dialect == "oracle":
            dbName = "Oracle"
        elif myThread.dialect == "mysql":
            dbNmae = "MySQL"
        elif myThread.dialect == "sqlite":
            dbName = "SQLite"
        else:
            dbName = myThread.dialect
            
        queryDialect = dbName + ".Queries"
        factory = WMFactory("alerts", "WMCore.Alerts")
        self.query = factory.loadObject(queryDialect)

    def publishAlert(self, severity, component, message):
        """
        _publishAlert_

        Publish an alert so that it gets added to the alert_current table.
        """
        self.query.publishAlert(severity, component, message)
        return

    def ackAlert(self, alertID):
        """
        _ackAlert_

        Acknowledge an alert which will move it from the alert_current table
        to the alert_history table.
        """
        self.query.ackAlert(alertID)
        return

    def listCurrentAlerts(self):
        """
        _listCurrentAlerts_

        Retrieve a list of the current alerts in the alert system.
        """
        alerts = self.query.listCurrentAlerts()

        alertList = []
        for alert in alerts:
            newAlert = Alert()
            newAlert["id"] = alert[0]
            newAlert["severity"] = alert[1]            
            newAlert["component"] = alert[2]
            newAlert["message"] = alert[3]
            newAlert["timestamp"] = alert[4]
            alertList.append(newAlert)

        return alertList
