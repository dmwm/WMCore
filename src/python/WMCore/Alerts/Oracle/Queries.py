#!/usr/bin/env python
"""
_Queries_

Queries for the alert system for the Oracle database.
"""

__revision__ = "$Id: Queries.py,v 1.1 2009/07/10 21:45:34 sryu Exp $"
__version__ = "$Revision: 1.1 $"

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
        sqlQuery = """SELECT ID, SEVERITY, COMPONENT, MESSAGE, GENERATIONTIME,
                          HISTORYTIME
                      FROM alert_history ORDER BY GENERATIONTIME DESC ROWNUM <= %d """ % max

        results = self.execute(sqlQuery, {}, conn, transaction)