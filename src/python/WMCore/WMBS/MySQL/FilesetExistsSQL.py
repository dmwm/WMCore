"""
MySQL implementation of FilesetExists
"""
from WMCore.WMBS.MySQL.Base import MySQLBase

class FilesetExists(MySQLBase):
    sql = """select count(*) from wmbs_fileset where name = :fileset"""
                
    def getBinds(self, fileset = None):
        return self.dbi.buildbinds(self.dbi.makelist(fileset), 'fileset')
    
    def format(self, result):
        if result[0].fetchone()[0] > 0:
            return True
        else:
            return False
    
    def execute(self, fileset = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, self.getBinds(fileset = fileset), 
                         conn = conn, transaction = transaction)
        return self.format(result)