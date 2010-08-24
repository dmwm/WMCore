from WMCore.WMBS.MySQL.Base import MySQLBase

class ListFileset(MySQLBase):
    sql = """select id, open, last_update from wmbs_fileset 
            where name = :fileset"""
            
    def getBinds(self, fileset = None):
        return self.dbi.buildbinds(self.dbi.makelist(fileset), 'fileset')
    
    def execute(self, fileset = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, self.getBinds(fileset), 
                         conn = conn, transaction = transaction)
        return self.format(result)