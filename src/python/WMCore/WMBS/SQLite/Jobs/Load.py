"""
SQLite implementation of Jobs.Load
"""
from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Jobs.Load import Load as LoadJobMySQL

class Load(LoadJobMySQL, SQLiteBase):
    sql = LoadJobMySQL.sql