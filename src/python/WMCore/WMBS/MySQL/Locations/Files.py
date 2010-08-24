#!/usr/bin/env python
"""
_Files_

MySQL implementation of Locations.Delete

"""
__all__ = []
__revision__ = "$Id: Files.py,v 1.3 2008/11/24 21:47:08 sryu Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter

class Delete(DBFormatter):
    sql = """select id, lfn, size, events, run, lumi from wmbs_file_details 
                where id in (select file from wmbs_file_location where location =
                    (select id from wmbs_location where se_name = :location))
        """
        
    def getBinds(self, location = None):
        return self.dbi.buildbinds(self.dbi.makelist(location), 'location')
        
    def execute(self, sename = None, conn = None, transaction = False):
        self.dbi.processData(self.sql, self.getBinds(location=sename), 
                         conn = conn, transaction = transaction)
        
        return self.format(result)
