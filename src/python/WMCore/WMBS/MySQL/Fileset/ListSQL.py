#!/usr/bin/env python
"""
_List_

MySQL implementation of ListFileset

"""
__all__ = []
__revision__ = "$Id: ListSQL.py,v 1.1 2008/06/09 16:30:08 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Base import MySQLBase

class List(MySQLBase):
    sql = "select * from wmbs_fileset order by last_update, name"
    
    def format(self, result):
        """
        Some standardised formatting
        """
        out = []
        for r in result:
            for i in r.fetchall():
                res = i
                j = int(i[0]), i[1], self.truefalse(i[2]), self.convertdatetime(i[3])
                out.append(j)
        return out