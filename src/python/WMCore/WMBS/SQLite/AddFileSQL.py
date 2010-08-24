"""
SQLite implementation of AddFile
"""
from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.AddFileSQL import AddFile as AddFileMySQL

class AddFile(AddFileMySQL, SQLiteBase):
    sql = AddFileMySQL.sql