#!/usr/bin/env python
"""
_GetLocation_

MySQL implementation of File.GetLocation
"""

from WMCore.Database.DBFormatter import DBFormatter


class GetLocation(DBFormatter):
    sql = """SELECT DISTINCT wpnn.pnn AS pnn FROM wmbs_file_location wfl
               INNER JOIN wmbs_file_details wfd ON wfd.id = wfl.fileid
               INNER JOIN wmbs_pnns wpnn ON wfl.pnn = wpnn.id
               WHERE wfd.lfn = :lfn"""

    def getBinds(self, file=None):
        binds = []
        file = self.dbi.makelist(file)
        for f in file:
            binds.append({'lfn': f})
        return binds

    def format(self, result):
        "Return a list of SE FQDN's"
        out = set()
        for r in result:
            for i in r.fetchall():
                out.add(i[0])
            r.close()
        return out

    def execute(self, file=None, conn=None, transaction=False):
        binds = self.getBinds(file)

        result = self.dbi.processData(self.sql, binds,
                                      conn=conn, transaction=transaction)
        return self.format(result)
