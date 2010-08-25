"""
Oracle implementation of ChildIDsByID
"""
from sets import Set

from WMCore.WMBS.MySQL.Files.GetChildIDsByID import GetChildIDsByID \
     as GetChildIDsMySQL

class GetChildIDsByID(GetChildIDsMySQL):
    sql = GetChildIDsMySQL.sql