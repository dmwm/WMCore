#!/usr/bin/env python
#pylint: disable-msg=E1103
"""

_Queries_

This module implements the SQLite backend for the trigger.

"""

__revision__ = "$Id: Queries.py,v 1.1 2009/05/14 15:46:13 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "mnorman@fnal.gov"

import threading

from WMCore.Trigger.MySQL.Queries import Queries as MySQLQueries

class Queries(MySQLQueries):
    """
    _Queries_
    
    This module implements the mysql backend for the trigger.
    
    """

    def TriggerQueriesDialetc(self):

        return 'SQLite'
