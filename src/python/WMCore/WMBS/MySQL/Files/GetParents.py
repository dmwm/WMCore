"""
MySQL implementation of File.GetParents

Return a list of lfn's which are parents for a file.
"""
from WMCore.WMBS.MySQL.Base import MySQLBase

class GetParents(MySQLBase):
    sql = """select lfn from wmbs_file_details where id IN (
        select parent from wmbs_file_parent where child = :file)"""
        
    def getBinds(self, files=None):
        binds = []
        files = self.dbi.makelist(files)
        for f in files:
            binds.append({'file': f})
        return binds
    
    def format(self, result):
        out = [] 
        for r in result:
            for f in r.fetchall():
                out.append(f)
        print out
        return out 
        
    def execute(self, files=None, conn = None, transaction = False):
        binds = self.getBinds(files)
        
        self.logger.debug('File.GetParents binds: %s' % binds)
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        self.logger.debug('File.GetParents result: %s' % result)
        return self.format(result)