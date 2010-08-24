#!/usr/bin/env python
"""
_Files_

MySQL implementation of Locations.Files

list the details of files in a given location 

"""
__all__ = []
__revision__ = "$Id: Files.py,v 1.4 2008/12/05 21:01:10 sryu Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.Database.DBFormatter import DBFormatter

class Files(DBFormatter):
    sql = """select id, lfn, size, events, run, lumi from wmbs_file_details 
                where id in (select file from wmbs_file_location where location =
                    (select id from wmbs_location where se_name = :location))
        """
        
    def getBinds(self, location = None):
        return self.dbi.buildbinds(self.dbi.makelist(location), 'location')
        
    def execute(self, sename = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, self.getBinds(location=sename), 
                         conn = conn, transaction = transaction)
        
        return self.format(result)
