"""
SQLite implementation of Files.InFileset
"""
from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Subscriptions.ForFileset import ForFileset as ForFilesetMySQL

class ForFileset(ForFilesetMySQL, SQLiteBase):
    sql = ForFilesetMySQL.sql
