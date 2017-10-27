#!/usr/bin/env python
"""
_SetLocationByLFN_

MySQL implementation of Files.SetLocationByLFN
"""




from WMCore.Database.DBFormatter import DBFormatter

class SetLocationByLFN(DBFormatter):
    sql = """INSERT INTO wmbs_file_location (fileid, location)
             VALUES ((SELECT id FROM wmbs_file_details where lfn = :lfn), :location)"""

    def getBinds(self, lfn = None, location = None):
        if isinstance(lfn, basestring):
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

        self.dbi.processData(self.sql, binds, conn=conn, transaction = transaction)
        return
