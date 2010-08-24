"""
SQLite implementation of ChildIDsByID
"""

from WMCore.WMBS.MySQL.Files.GetChildIDsByID import GetChildIDsByID \
     as GetChildIDsMySQL

class GetChildIDsByID(GetChildIDsMySQL):
    sql = GetChildIDsMySQL.sql