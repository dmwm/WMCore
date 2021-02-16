#!/usr/bin/env python
"""
_SetLocationByLFN_

MySQL implementation of Files.SetLocationByLFN
"""

from WMCore.Database.DBFormatter import DBFormatter

from builtins import str, bytes

class SetLocationByLFN(DBFormatter):
    sql = """INSERT INTO wmbs_file_location (fileid, pnn)
                 SELECT wfd.id, wpnn.id
                 FROM wmbs_pnns wpnn, wmbs_file_details wfd
                 WHERE wpnn.pnn = :location
                 AND wfd.lfn = :lfn"""

    def getBinds(self, lfn=None, location=None):
        if isinstance(lfn, (str, bytes)):
            return {'lfn': lfn, 'location': location}
        elif isinstance(lfn, (list, set)):
            binds = []
            for bind in lfn:
                binds.append(bind)
            return binds

    def execute(self, lfn, location=None, conn=None, transaction=None):
        """
        Set location PNN by LFN

        """
        binds = self.getBinds(lfn, location)

        self.dbi.processData(self.sql, binds, conn=conn, transaction=transaction)
        return
