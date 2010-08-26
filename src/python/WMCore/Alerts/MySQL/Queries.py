#!/usr/bin/env python
"""
_Queries_

Queries for the alert system for the MySQL database.
"""

__revision__ = "$Id: Queries.py,v 1.3 2009/10/23 19:57:53 sryu Exp $"
__version__ = "$Revision: 1.3 $"

import threading
import time

from WMCore.Database.DBFormatter import DBFormatter

class Queries(DBFormatter):
    """
    _Queries_

    Queries for the alert system for the MySQL database.    
    """
    def __init__(self):
        myThread = threading.currentThread()
        DBFormatter.__init__(self, myThread.logger, myThread.dbi)
        
    def execute(self, sql, binds, conn = None, transaction = False):
        """
        A simple select with no binds/arguments is the default
        """
        result = self.dbi.processData(sql, binds, 
                         conn = conn, transaction = transaction)
        return self.format(result)

    def publishAlert(self, severity, component, message, conn = None, transaction = False):
        """
        _publishAlert_

        Publish a new alert with a given severity, component and message.
        """
        sqlQuery = """INSERT INTO alert_current (SEVERITY, COMPONENT, MESSAGE, TIME)
                      VALUES (:p_1, :p_2, :p_3, :p_4)"""
        bindVars = {"p_1": severity, "p_2": component, "p_3": message, "p_4": int(time.time())}
        
        self.execute(sqlQuery, bindVars, conn, transaction)
    
        return

    def ackAlert(self, alertID, conn = None, transaction = False):
        """
        _ackAlert_

        Given the ID of a current alert, remove it from the current table and
        add it to the history table.
        """
        insertQuery = """INSERT INTO alert_history (SEVERITY, COMPONENT,
                         MESSAGE, GENERATIONTIME, HISTORYTIME) VALUES (SELECT SEVERITY,
                         COMPONENT, MESSAGE, TIME, :p_2 FROM alert_current
                         WHERE ID = :p_1)"""
        deleteQuery = "DELETE FROM alert_current WHERE ID = :p_1"
        bindVars = {"p_1": alertID, "p_2": int(time.time())}
        
        self.execute(insertQuery, bindVars)
        self.execute(deleteQuery, bindVars)

    def listCurrentAlerts(self, conn = None, transaction = False):
        """
        _listCurrentAlerts_

        Retrieve a list of all the alerts in the alerts_current table.  Results
        are returned in the form of a list of tuples where data in each tuple
        is ID, SEVERITY, COMPONENT, MESSAGE and TIME.
        """
        sqlQuery = """SELECT ID, SEVERITY, COMPONENT, MESSAGE, TIME
                      FROM alert_current"""

        results = self.execute(sqlQuery, {}, conn, transaction)

        return results

    def listPastAlerts(self, max = 10, conn = None, transaction = False):
        """
        _listPastAlerts_

        """
        sqlQuery = """SELECT ID, SEVERITY, COMPONENT, MESSAGE, GENERATIONTIME,
                          HISTORYTIME
                      FROM alert_history ORDER BY GENERATIONTIME DESC LIMIT %d""" % max

        results = self.execute(sqlQuery, {}, conn, transaction)

        return results