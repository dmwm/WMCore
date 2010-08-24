"""
MySQL implementation of ListFileset
"""
from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.ListFilesetSQL import ListFileset

class ListFileset(SQLiteBase):
    sql = ListFileset.sql
    
    