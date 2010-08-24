#!/usr/bin/env python
"""
_Load_

SQLite implementation of LoadFileset

"""
__all__ = []
__revision__ = "$Id: Load.py,v 1.1 2008/06/12 10:02:07 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Fileset.Load import Load as LoadFilesetMySQL

class Load(LoadFilesetMySQL, SQLiteBase):
    sql = LoadFilesetMySQL.sql
    
    