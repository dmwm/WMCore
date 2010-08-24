"""
SQLite implementation of SetFileLocation
"""

from WMCore.WMBS.MySQL.Files.SetLocation import SetLocation as SetLocationMySQL

class SetLocation(SetLocationMySQL):
    sql = """insert into wmbs_file_location (file, location) 
                values ((select id from wmbs_file_details where lfn = :file),
                (select id from wmbs_location where se_name = :location))"""
                
    def getBinds(self, file = None, location = None):
        if type(location) == type('string'):
            return self.dbi.buildbinds(self.dbi.makelist(file), 'file', 
                   self.dbi.buildbinds(self.dbi.makelist(location), 'location'))
        elif type(location) == type([]):
            binds = []
            for l in location:
                binds.extend(self.dbi.buildbinds(self.dbi.makelist(file), 'file', 
                   self.dbi.buildbinds(self.dbi.makelist(l), 'location')))
            return binds
        
    def execute(self, file = None, sename = None, conn = None, transaction = False):
        binds = self.getBinds(file, sename)
        self.logger.debug('File.SetLocation binds: %s' % binds)
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        
        return self.format(result)