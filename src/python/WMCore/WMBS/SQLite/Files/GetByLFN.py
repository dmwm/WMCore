"""
SQLite implementation of GetFile
"""
from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Files.GetByLFN import GetByLFN as GetFileMySQL

class GetByLFN(GetFileMySQL, SQLiteBase):
    sql = GetFileMySQL.sql