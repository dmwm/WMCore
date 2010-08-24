"""
SQLite implementation of Files.Load
"""
from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Subscriptions.Load import Load as LoadMySQL

class Load(LoadMySQL, SQLiteBase):
    sql = LoadMySQL.sql