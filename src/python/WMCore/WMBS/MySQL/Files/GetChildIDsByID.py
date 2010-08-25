"""
MySQL implementation of File.GetChildIDsByID

Return a list of ids which are children for a file(s) with a given id(s).
"""
from sets import Set
from WMCore.Database.DBFormatter import DBFormatter

class GetChildIDsByID(DBFormatter):
    sql = """select distinct child from wmbs_file_parent where parent = :parent"""
        
    def getBinds(self, ids=None):
        binds = []
        childIDs = self.dbi.makelist(ids)
        for id in childIDs:
            binds.append({'parent': id})
        return binds
    
    def format(self, result):        
        out = Set() 
        for r in result:
            for f in r.fetchall():
                out.add(f[0])
            r.close()
        return list(out) 
        
    def execute(self, ids=None, conn = None, transaction = False):
        binds = self.getBinds(ids)
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return self.format(result)