#!/usr/bin/env python
#pylint: disable-msg=E1103
"""
_Queries_

This module implements the oracle backend for the trigger.

"""

__revision__ = \
    "$Id: Queries.py,v 1.1 2008/09/19 15:34:36 fvlingen Exp $"
__version__ = \
    "$Revision: 1.1 $"
__author__ = \
    "fvlingen@caltech.edu"

from WMCore.Trigger.MySQL.Queries import Queries as MySQLQueries

class Queries(MySQLQueries):
    """
    _Queries_
    
    This module implements the oracle backend for the trigger.
    As much as possible we inherit from the mysql queries.
    
    """
    
    def __init__(self):
        MySQLQueries.__init__(self)

    def lockTrigger(self, args):
        """
        Locks a row in the table to prevent concurrent writes.
        """
        # oracle can not handle a whole list of updates only one by one.
        for arg in args:
            sqlStr = """
SELECT * FROM tr_trigger WHERE trigger_id = :trigger_id AND id = :id FOR UPDATE
            """
            self.execute(sqlStr, arg)

