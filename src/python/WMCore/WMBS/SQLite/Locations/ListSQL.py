#!/usr/bin/env python
"""
_List_

SQLite implementation of ListLocations

"""
__all__ = []
__revision__ = "$Id: ListSQL.py,v 1.1 2008/06/10 11:55:59 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Locations.ListSQL import List as ListLocationsMySQL

class List(ListLocationsMySQL, SQLiteBase):
    sql = ListLocationsMySQL.sql
    
    def format(self, result):
        """
        Some standardised formatting
        """
        out = []
        for r in result:
            for i in r.fetchall():
                res = i
                j = i[0], str(i[1])
                out.append(j)
        return out