#!/usr/bin/env python
"""
_GetLocationBulk_

MySQL implementation of File.GetLocationBulk
"""




from WMCore.Database.DBFormatter import DBFormatter

class GetLocationBulk(DBFormatter):
    sql = """SELECT wls.se_name AS se_name, wfl.fileid AS id FROM wmbs_location wl
                INNER JOIN wmbs_file_location wfl ON wfl.location = wl.id
                INNER JOIN wmbs_location_senames wls ON wls.location = wl.id
                WHERE wfl.fileid = :id
                """


    def format(self, rawResults):
        """
        _format_

        Group files into single entries
        """

        results    = {}

        for raw in rawResults:
            if not raw['id'] in results.keys():
                results[raw['id']] = []
            results[raw['id']].append(raw['se_name'])

        return results



    def execute(self, files = [], conn = None, transaction = False):

        if len(files) == 0:
            # Nothing to do
            return

        binds = []
        for fid in files:
            binds.append({'id': fid})

        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)

        return self.format(self.formatDict(result))
