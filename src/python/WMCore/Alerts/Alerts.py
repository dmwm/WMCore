#!/usr/bin/env python
"""
_Alerts_

WMCore alert system.
"""

__revision__ = "$Id: Alerts.py,v 1.2 2008/10/23 19:16:47 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.DataStructs.Alert import Alert
from WMCore.WMFactory import WMFactory
from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction
from WMCore.WMException import WMException

import threading
import os
import logging

class Alerts:
    """
    _Alerts_

    WMCore alert system.
    """
    def __init__(self):
        """
        ___init___

        Setup the database connections and try to create the tables that are
        needed for the alert system.  This expects the following environment
        variables to be defined:
          DIALECT - Currently must be MySQL
          DATABASE - A connection string to the MySQL database
          DBSOCK - Path the the MySQL socket
        """
        myThread = threading.currentThread()
        myThread.dialect = os.getenv("DIALECT")
        myThread.logger = logging
                                                           
        options = {"unix_socket": os.getenv("DBSOCK")}
        dbFactory = DBFactory(myThread.logger, os.getenv("DATABASE"), options)
        myThread.dbi = dbFactory.connect()
        myThread.transaction = Transaction(myThread.dbi)
                                                            
        alertDAOFactory = WMFactory("alerts", "WMCore.Alerts")

        try:
            alertCreate = alertDAOFactory.loadObject(myThread.dialect + ".Create")
            alertCreate.execute(conn = myThread.transaction.conn)
            createTransaction.commit()
        except WMException, ex:
            logging.debug("Looks like the alert tables already exists...")
        
        self.query = alertDAOFactory.loadObject(myThread.dialect + ".Queries")

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
