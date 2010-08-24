"""
SQLite implementation of GetLocationFile
"""

from WMCore.WMBS.MySQL.Files.GetLocation import GetLocation as GetLocationFileMySQL

class GetLocation(GetLocationFileMySQL):
    sql = GetLocationFileMySQL.sql