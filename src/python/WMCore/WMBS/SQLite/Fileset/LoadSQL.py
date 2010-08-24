#!/usr/bin/env python
"""
_Load_

SQLite implementation of LoadFileset

"""
__all__ = []
__revision__ = "$Id: LoadSQL.py,v 1.1 2008/06/09 16:30:08 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Fileset.LoadSQL import Load as LoadFilesetMySQL

class Load(LoadFilesetMySQL, SQLiteBase):
    sql = LoadFilesetMySQL.sql
    
    