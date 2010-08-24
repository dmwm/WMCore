"""
SQLite implementation of Jobs.AddFiles
"""
from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Jobs.AddFiles import AddFiles as AddFilesJobMySQL

class AddFiles(AddFilesJobMySQL, SQLiteBase):
    sql = AddFilesJobMySQL.sql