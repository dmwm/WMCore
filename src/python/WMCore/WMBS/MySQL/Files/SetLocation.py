"""
MySQL implementation of SetLocation
"""
from WMCore.Database.DBFormatter import DBFormatter

class SetLocation(DBFormatter):
    
    #NSERT newtable (user,age,os) SELECT table1.user,table1.age,table2.os FROM table1,table2 WHERE table1.user=table2.user;
    
    sql = """insert wmbs_file_location (file, location) 
            select wmbs_file_details.id, wmbs_location.id from wmbs_file_details, wmbs_location 
            where wmbs_file_details.lfn = :file
            and wmbs_location.se_name = :location"""
                
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