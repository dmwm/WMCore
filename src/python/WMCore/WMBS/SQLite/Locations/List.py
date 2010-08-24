#!/usr/bin/env python
"""
_List_

SQLite implementation of ListLocations

"""
__all__ = []
__revision__ = "$Id: List.py,v 1.2 2008/11/20 21:54:28 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Locations.List import List as ListLocationsMySQL

class List(ListLocationsMySQL):
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