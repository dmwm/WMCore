#!/usr/bin/env python
"""
_Load_

SQLite implementation of LoadFileset

"""
__all__ = []
__revision__ = "$Id: Load.py,v 1.2 2008/06/24 16:23:09 metson Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Fileset.Load import Load as LoadFilesetMySQL

class Load(LoadFilesetMySQL, SQLiteBase):
    sql = LoadFilesetMySQL.sql
    
    def format(self, result):
        result = result[0].fetchall()[0]
        time = result[2]
        open = result[1]
        id = int(result[0])
        return id, open, time