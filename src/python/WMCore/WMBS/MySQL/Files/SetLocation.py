"""
MySQL implementation of SetLocation
"""
from WMCore.Database.DBFormatter import DBFormatter
from sets import Set

class SetLocation(DBFormatter):
    sql = """insert wmbs_file_location (file, location) 
             select wmbs_file_details.id, wmbs_location.id from wmbs_file_details, wmbs_location 
             where wmbs_file_details.lfn = :lfn
             and wmbs_location.se_name = :location"""
                
    def getBinds(self, file = None, location = None):
        if type(location) == type('string'):
            return self.dbi.buildbinds(self.dbi.makelist(file), 'lfn', 
                   self.dbi.buildbinds(self.dbi.makelist(location), 'location'))
        elif isinstance(location, (list, Set, set)):
            binds = []
            for l in location:
                binds.extend(self.dbi.buildbinds(self.dbi.makelist(file), 'lfn', 
                   self.dbi.buildbinds(self.dbi.makelist(l), 'location')))
            return binds
        else:
            raise Exception, "Type of location argument is not allowed: %s" \
                                % type(location)
    
    def format(self, result):
        return True
        
    def execute(self, file, location, conn = None, transaction = None):
        binds = self.getBinds(file, location)

        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        
        return self.format(result)
