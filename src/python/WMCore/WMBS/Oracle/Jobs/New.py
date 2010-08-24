"""
SQLite implementation of Jobs.New
"""
from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Jobs.New import New as NewJobMySQL

class New(NewJobMySQL, SQLiteBase):
    sql = NewJobMySQL.sql