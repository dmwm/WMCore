#!/usr/bin/env python
"""
_SetLocation_

MySQL implementation of DBSBuffer.SetLocation
"""

from WMCore.Database.DBFormatter import DBFormatter
from sets import Set

class SetLocation(DBFormatter):
    sql = """INSERT INTO dbsbuffer_file_location (filename, location) 
               SELECT dbsbuffer_file.id, dbsbuffer_location.id from dbsbuffer_file, dbsbuffer_location 
             WHERE dbsbuffer_file.lfn = :lfn
             AND dbsbuffer_location.se_name = :location
             AND NOT EXISTS (SELECT * FROM dbsbuffer_file_location WHERE filename = dbsbuffer_file.id
             and location = dbsbuffer_location.id)"""
                
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
    
    def execute(self, file, location, conn = None, transaction = None):
        binds = self.getBinds(file, location)
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        return
