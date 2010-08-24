"""
SQLite implementation of AddFile
"""
from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Files.Add import Add as AddFileMySQL

class Add(AddFileMySQL, SQLiteBase):
    sql = AddFileMySQL.sql