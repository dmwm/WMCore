"""
Oracle implementation of ChildIDsByID
"""
from sets import Set

from WMCore.WMBS.MySQL.Files.GetChildIDsByID import GetChildIDsByID \
     as GetChildIDsMySQL

class GetChildIDsByID(GetChildIDsMySQL):
    sql = GetChildIDsMySQL.sql
    
    def format(self, result):        
        out = Set() 
        for r in result:
            for f in r.fetchall():
                out.add(f[0])
            r.close()
        return list(out)