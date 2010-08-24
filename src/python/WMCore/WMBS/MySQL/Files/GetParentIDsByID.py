"""
MySQL implementation of File.GetParentIDsByID

Return a list of ids which are parents for a file with a given id(s).
"""
from sets import Set
from WMCore.Database.DBFormatter import DBFormatter

class GetParentIDsByID(DBFormatter):
    sql = """select distinct parent from wmbs_file_parent where child = :child"""
        
    def getBinds(self, ids=None):
        binds = []
        childIDs = self.dbi.makelist(ids)
        for id in childIDs:
            binds.append({'child': id})
        return binds
    
    def format(self, result):        
        out = Set() 
        for r in result:
            for f in r.fetchall():
                out.add(f[0])
        return list(out) 
        
    def execute(self, ids=None, conn = None, transaction = False):
        binds = self.getBinds(ids)
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return self.format(result)