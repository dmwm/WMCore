#!/usr/bin/env python
"""
_Load_

SQLite implementation of LoadFileset

"""
__all__ = []
__revision__ = "$Id: LoadFromID.py,v 1.1 2008/10/08 14:30:11 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Fileset.LoadFromID import LoadFromID as LoadFilesetMySQL

class LoadFromID(LoadFilesetMySQL, SQLiteBase):
    sql = LoadFilesetMySQL.sql
    
    def format(self, result):
        result = result[0].fetchall()[0]
        time = result[2]
        open = result[1]
        name = result[0]
        return name, open, time