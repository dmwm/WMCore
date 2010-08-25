"""
Oracle implementation of GetParentIDsByID
"""
from sets import Set
from WMCore.WMBS.MySQL.Files.GetParentIDsByID import GetParentIDsByID \
     as GetParentIDsMySQL

class GetParentIDsByID(GetParentIDsMySQL):
    sql = GetParentIDsMySQL.sql
    
    def format(self, result):        
        out = Set() 
        for r in result:
            for f in r.fetchall():
                out.add(f[0])
        return list(out) 
    
