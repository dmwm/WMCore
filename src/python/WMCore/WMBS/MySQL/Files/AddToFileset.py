"""
MySQL implementation of Files.AddToFileset
"""
from WMCore.WMBS.MySQL.Base import MySQLBase

class AddToFileset(MySQLBase):
    sql = """
            insert into wmbs_fileset_files (file, fileset) 
                values ((select id from wmbs_file_details where lfn = :file),
                (select id from wmbs_fileset where name = :fileset))"""
        
    def getBinds(self, file = None, fileset = None):
        return self.dbi.buildbinds(self.dbi.makelist(fileset), 'fileset',
                                    self.dbi.buildbinds(self.dbi.makelist(file), 'file'))
            
    def execute(self, file=None, fileset=None, conn = None, transaction = False):
        binds = self.getBinds(file, fileset)
        
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return self.format(result)