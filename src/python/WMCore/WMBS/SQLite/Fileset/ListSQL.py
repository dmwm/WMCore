#!/usr/bin/env python
"""
_List_

SQLite implementation of ListFileset

"""
__all__ = []
__revision__ = "$Id: ListSQL.py,v 1.1 2008/06/09 16:30:08 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Fileset.ListSQL import List as ListFilesetMySQL

class List(ListFilesetMySQL, SQLiteBase):
    sql = ListFilesetMySQL.sql
    
    def format(self, result):
        """
        Some standardised formatting
        """
        out = []
        for r in result:
            for i in r.fetchall():
                res = i
                j = i[0], str(i[1]), self.truefalse(i[2]), i[3]
                out.append(j)
        return out