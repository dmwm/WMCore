#!/usr/bin/env python
"""
_List_

MySQL implementation of ListFileset

"""
__all__ = []
__revision__ = "$Id: List.py,v 1.4 2008/11/26 19:46:48 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.Database.DBFormatter import DBFormatter

class List(DBFormatter):
    sql = "select id, name, open, last_update from wmbs_fileset order by last_update, name"
    
    def format(self, result):
        """
        Some standardised formatting
        """
        out = []
        for r in result:
            for i in r.fetchall():
                res = i
                j = int(i[0]), i[1], self.truefalse(i[2]), i[3]
                out.append(j)
        return out
