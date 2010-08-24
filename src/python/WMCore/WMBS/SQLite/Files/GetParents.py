"""
SQLite implementation of GetParentsFile
"""
from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Files.GetParents import GetParents as GetParentsFileMySQL

class GetParents(GetParentsFileMySQL, SQLiteBase):
    sql = GetParentsFileMySQL.sql