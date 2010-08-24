#!/usr/bin/env python
"""
_List_

MySQL implementation of ListFileset

"""
__all__ = []
__revision__ = "$Id: List.py,v 1.2 2008/06/16 16:05:42 metson Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Base import MySQLBase

class List(MySQLBase):
    sql = "select id, se_name from wmbs_location order by se_name"
    
    def format(self, result):
        """
        Some standardised formatting
        """
        out = []
        for r in result:
            for i in r.fetchall():
                res = i
                j = int(i[0]), i[1]
                out.append(j)
        return out