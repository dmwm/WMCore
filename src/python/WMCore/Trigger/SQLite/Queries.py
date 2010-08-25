#!/usr/bin/env python
#pylint: disable-msg=E1103
"""

_Queries_

This module implements the SQLite backend for the trigger.

"""





import threading

from WMCore.Trigger.MySQL.Queries import Queries as MySQLQueries

class Queries(MySQLQueries):
    """
    _Queries_
    
    This module implements the mysql backend for the trigger.
    
    """

    def TriggerQueriesDialetc(self):

        return 'SQLite'
