"""
MySQL implementation of Files.InFileset
"""
from WMCore.WMBS.MySQL.Base import MySQLBase

class InFileset(MySQLBase):
    sql = """select id, lfn, size, events, run, lumi from wmbs_file_details 
                where id in (select file from wmbs_fileset_files where 
                fileset = (select id from wmbs_fileset where name = :fileset))"""
                
    def getBinds(self, fileset = None):
        return self.dbi.buildbinds(self.dbi.makelist(fileset), 'fileset')
            
    def execute(self, fileset=None, conn = None, transaction = False):
        binds = self.getBinds(fileset)
        
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return self.format(result)