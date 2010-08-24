"""
SQLite implementation of GetFile
"""

from WMCore.WMBS.MySQL.Files.GetByID import GetByID as GetFileMySQL

class GetByID(GetFileMySQL):
    sql = GetFileMySQL.sql