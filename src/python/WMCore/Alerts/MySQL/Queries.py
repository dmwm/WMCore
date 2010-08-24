#!/usr/bin/env python
"""
_Queries_

Queries for the alert system for the MySQL database.
"""

__revision__ = "$Id: Queries.py,v 1.1 2008/10/22 21:31:09 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

import threading

from WMCore.Database.DBFormatter import DBFormatter

class Queries(DBFormatter):
    """
    _Queries_

    Queries for the alert system for the MySQL database.    
    """
    def __init__(self):
        myThread = threading.currentThread()
        DBFormatter.__init__(self, myThread.logger, myThread.dbi)

    def publishAlert(self, severity, component, message):
        """
        _publishAlert_

        Publish a new alert with a given severity, component and message.
        """
        sqlQuery = """INSERT INTO alert_current (SEVERITY, COMPONENT, MESSAGE)
                      VALUES (:p_1, :p_2, :p_3)"""
        bindVars = {"p_1": severity, "p_2": component, "p_3": message}
        
        myThread = threading.currentThread()
        myThread.transaction.begin()
        myThread.transaction.processData(sqlQuery, bindVars)
        myThread.transaction.commit()

        return

    def ackAlert(self, alertID):
        """
        _ackAlert_

        Given the ID of a current alert, remove it from the current table and
        add it to the history table.
        """
        insertQuery = """INSERT INTO alert_history (SEVERITY, COMPONENT,
                         MESSAGE, GENERATIONTIME) VALUES (SELECT SEVERITY,
                         COMPONENT, MESSAGE, TIME FROM alert_current
                         WHERE ID = :p_1)"""
        deleteQuery = "DELETE FROM alert_current WHERE ID = :p_1"
        bindVars = {"p_1": alertID}

        myThread = threading.currentThread()
        myThread.transaction.begin()
        myThread.transaction.processData(insertQuery, bindVars)
        myThread.transaction.processData(deleteQuery, bindVars)        
        myThread.transaction.commit()        

    def listCurrentAlerts(self):
        """
        _listCurrentAlerts_

        Retrieve a list of all the alerts in the alerts_current table.  Results
        are returned in the form of a list of tuples where data in each tuple
        is ID, SEVERITY, COMPONENT, MESSAGE and TIME.
        """
        sqlQuery = """SELECT ID, SEVERITY, COMPONENT, MESSAGE, TIME
                      FROM alert_current"""

        myThread = threading.currentThread()
        myThread.transaction.begin()
        resultProxy = myThread.transaction.processData(sqlQuery)
        myThread.transaction.commit()

        return self.format(resultProxy)

    def listPastAlerts(self, max = 10):
        """
        _listPastAlerts_

        """
        pass
