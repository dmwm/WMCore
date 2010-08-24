#!/usr/bin/env python
"""
_SetLocation_

MySQL implementation of Files.SetLocation
"""




from WMCore.Database.DBFormatter import DBFormatter

class SetLocation(DBFormatter):
    sql = """INSERT INTO wmbs_file_location (file, location) 
             SELECT :fileid, wmbs_location.id FROM wmbs_location 
             WHERE wmbs_location.se_name = :location"""
                
    def getBinds(self, file = None, location = None):
        if type(location) == type('string'):
            return self.dbi.buildbinds(self.dbi.makelist(file), 'fileid', 
                   self.dbi.buildbinds(self.dbi.makelist(location), 'location'))
        elif isinstance(location, (list, set)):
            binds = []
            for l in location:
                binds.extend(self.dbi.buildbinds(self.dbi.makelist(file), 'fileid', 
                   self.dbi.buildbinds(self.dbi.makelist(l), 'location')))
            return binds
        else:
            raise Exception, "Type of location argument is not allowed: %s" \
                                % type(location)
    
    def execute(self, file, location, conn = None, transaction = None):
        binds = self.getBinds(file, location)

        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        return
