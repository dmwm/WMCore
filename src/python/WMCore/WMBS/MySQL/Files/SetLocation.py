"""
MySQL implementation of SetLocation
"""
from WMCore.WMBS.MySQL.Base import MySQLBase

class SetLocation(MySQLBase):
    sql = """insert into wmbs_file_location (file, location) 
                values ((select id from wmbs_file_details where lfn = :file),
                (select id from wmbs_location where se_name = :location))"""
                
    def getBinds(self, file = None, location = None):
        return self.dbi.buildbinds(self.dbi.makelist(file), 'file', 
                                   self.dbi.buildbinds(self.dbi.makelist(location), 'location'))
    
    def execute(self, file = None, sename = None, conn = None, transaction = False):
        binds = self.getBinds(file, sename)
        self.logger.debug('File.SetLocation binds: %s' % binds)
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        
        return self.format(result)