"""
SQLite implementation of GetFile
"""

from WMCore.WMBS.MySQL.Files.GetByLFN import GetByLFN as GetFileMySQL

class GetByLFN(GetFileMySQL):
    sql = GetFileMySQL.sql