"""
SQLite implementation of AddRunLumiFile
"""
from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Files.AddRunLumi import AddRunLumi as AddRunLumiMySQL

class AddRunLumi(AddRunLumiMySQL, SQLiteBase):
    sql = AddRunLumiMySQL.sql