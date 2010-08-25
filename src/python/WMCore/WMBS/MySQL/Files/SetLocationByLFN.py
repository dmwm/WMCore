#!/usr/bin/env python
"""
_SetLocationByLFN_

MySQL implementation of Files.SetLocationByLFN
"""

__revision__ = "$Id: SetLocationByLFN.py,v 1.2 2010/03/10 20:48:56 mnorman Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class SetLocationByLFN(DBFormatter):
    sql = """INSERT INTO wmbs_file_location (file, location) 
             SELECT wmbs_file_details.id, wmbs_location.id
               FROM wmbs_location, wmbs_file_details
               WHERE wmbs_location.site_name = :location
               AND wmbs_file_details.lfn = :lfn"""
                
    def getBinds(self, lfn = None, location = None):
        if type(lfn) == type('string'):
            return {'lfn': lfn, 'location': location}
        elif isinstance(lfn, (list, set)):
            binds = []
            for bind in lfn:
                binds.append(bind)
            return binds

    
    def execute(self, lfn, location = None, conn = None, transaction = None):
        """
        Set location by LFN

        """
        binds = self.getBinds(lfn, location)

        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        return
