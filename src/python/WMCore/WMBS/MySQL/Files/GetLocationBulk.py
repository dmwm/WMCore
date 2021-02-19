#!/usr/bin/env python
"""
_GetLocationBulk_

MySQL implementation of File.GetLocationBulk
"""

from WMCore.Database.DBFormatter import DBFormatter


class GetLocationBulk(DBFormatter):
    sql = """SELECT wpnn.pnn as pnn, wfl.fileid as id
               FROM wmbs_file_location wfl
               INNER JOIN wmbs_pnns wpnn ON wfl.pnn = wpnn.id
             WHERE wfl.fileid = :id
            """

    def format(self, rawResults):
        """
        _format_

        Group files into single entries
        """

        results = {}

        for raw in rawResults:
            if raw['id'] not in results:
                results[raw['id']] = []
            results[raw['id']].append(raw['pnn'])

        return results

    def execute(self, files=None, conn=None, transaction=False):
        files = files or []
        if len(files) == 0:
            # Nothing to do
            return

        binds = []
        for fid in files:
            binds.append({'id': fid})

        result = self.dbi.processData(self.sql, binds,
                                      conn=conn, transaction=transaction)

        return self.format(self.formatDict(result))
