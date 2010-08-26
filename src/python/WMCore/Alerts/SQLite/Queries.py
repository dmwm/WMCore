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