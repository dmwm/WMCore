"""
SQLite implementation of LoadFileset
"""
from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.LoadFilesetSQL import LoadFileset as LoadFilesetMySQL

class LoadFileset(LoadFilesetMySQL, SQLiteBase):
    sql = LoadFilesetMySQL.sql
    
    