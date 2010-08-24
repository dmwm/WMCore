"""
SQLite implementation of Files.Exists
"""
from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Subscriptions.Exists import Exists as ExistsMySQL

class Exists(ExistsMySQL, SQLiteBase):
    sql = ExistsMySQL.sql