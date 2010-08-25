"""
Oracle implementation of GetParentIDsByID
"""
from sets import Set
from WMCore.WMBS.MySQL.Files.GetParentIDsByID import GetParentIDsByID \
     as GetParentIDsMySQL

class GetParentIDsByID(GetParentIDsMySQL):
    sql = GetParentIDsMySQL.sql
    
