"""
Oracle implementation of GetParentsFile
"""

from WMCore.WMBS.MySQL.Files.GetParents import GetParents as GetParentsFileMySQL

class GetParents(GetParentsFileMySQL):
    sql = GetParentsFileMySQL.sql
