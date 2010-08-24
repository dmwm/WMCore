#!/usr/bin/env python
"""
_Load_

SQLite implementation of LoadFileset

"""
__all__ = []
__revision__ = "$Id: LoadFromName.py,v 1.1 2008/07/03 09:54:22 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Fileset.LoadFromName import LoadFromName as LoadFilesetMySQL

class LoadFromName(LoadFilesetMySQL, SQLiteBase):
    sql = LoadFilesetMySQL.sql
    
    def format(self, result):
        result = result[0].fetchall()[0]
        time = result[2]
        open = result[1]
        id = int(result[0])
        return id, open, time