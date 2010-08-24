"""
MySQL implementation of ListFileset
"""
from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.ListFilesetSQL import ListFileset as ListFilesetMySQL

class ListFileset(ListFilesetMySQL, SQLiteBase):
    sql = ListFilesetMySQL.sql
    
    