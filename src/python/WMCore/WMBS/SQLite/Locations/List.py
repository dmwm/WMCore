#!/usr/bin/env python
"""
_List_

SQLite implementation of ListLocations
"""

__all__ = []
__revision__ = "$Id: List.py,v 1.3 2009/05/09 11:42:27 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

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
