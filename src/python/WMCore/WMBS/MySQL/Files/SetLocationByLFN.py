#!/usr/bin/env python
"""
_SetLocationByLFN_

MySQL implementation of Files.SetLocationByLFN
"""




from WMCore.Database.DBFormatter import DBFormatter

class SetLocationByLFN(DBFormatter):
    sql = """INSERT INTO wmbs_file_location (fileid, location)
             SELECT wmbs_file_details.id, wls.location
               FROM wmbs_location_pnns wls, wmbs_file_details
               WHERE wls.se_name = :location
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
