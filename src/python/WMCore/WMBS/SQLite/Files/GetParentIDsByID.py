"""
SQLite implementation of GetParentIDsByID
"""

from WMCore.WMBS.MySQL.Files.GetParentIDsByID import GetParentIDsByID \
     as GetParentIDsMySQL

class GetParentIDsByID(GetParentIDsMySQL):
    sql = GetParentIDsMySQL.sql