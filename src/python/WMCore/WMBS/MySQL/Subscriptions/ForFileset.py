"""
MySQL implementation of Subscriptions.ForFileset
"""
from WMCore.Database.DBFormatter import DBFormatter

class ForFileset(DBFormatter):
    sql = """select id from wmbs_subscription
                where fileset IN (select id from wmbs_fileset where name = :fileset)
          """
                
    def format(self, result):
        result = DBFormatter.format(self, result)
        self.logger.debug( result )
        return [x[0] for x in result]
                
    def getBinds(self, fileset = None):
        return self.dbi.buildbinds(self.dbi.makelist(fileset), 'fileset')
            
    def execute(self, fileset=None, conn = None, transaction = False):
        binds = self.getBinds(fileset)
        
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return self.format(result)