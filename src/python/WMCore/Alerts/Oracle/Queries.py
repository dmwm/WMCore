#!/usr/bin/env python
"""
_Queries_

Queries for the alert system for the Oracle database.
"""

__revision__ = "$Id: Queries.py,v 1.2 2010/05/04 16:14:12 sryu Exp $"
__version__ = "$Revision: 1.2 $"

import threading

from WMCore.Alerts.MySQL.Queries import Queries as QueriesMySQL

class Queries(QueriesMySQL):
    """
    _Queries_

    Queries for the alert system for the Oracle database.    
    """
    
    def listPastAlerts(self, max = 10, conn = None, transaction = False):
        """
        _listPastAlerts_

        """
        sqlQuery = """SELECT id, severity, component, message, generationtime, historytime 
                       FROM (SELECT * FROM alert_history ORDER BY GENERATIONTIME DESC) 
                       WHERE ROWNUM <= %d """ % max

        results = self.execute(sqlQuery, {}, conn, transaction)