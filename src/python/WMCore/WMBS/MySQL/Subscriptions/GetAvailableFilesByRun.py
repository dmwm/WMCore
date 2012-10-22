#!/usr/bin/env python
"""
_GetAvailableFilesByRun_

MySQL implementation of Subscription.GetAvailableFilesByRun
"""

from WMCore.Database.DBFormatter import DBFormatter

class GetAvailableFilesByRun(DBFormatter):
    sql = """SELECT distinct(wmbs_sub_files_available.fileid) FROM wmbs_sub_files_available
               INNER JOIN wmbs_file_runlumi_map ON
                 wmbs_sub_files_available.fileid = wmbs_file_runlumi_map.fileid
             WHERE wmbs_sub_files_available.subscription = :subscription AND
                   wmbs_file_runlumi_map.run = :run"""

    def formatDict(self, results):
        """
        _formatDict_

        Cast the file column to an integer as the DBFormatter's formatDict()
        method turns everything into strings.  Also, fixup the results of the
        Oracle query by renaming "fileid" to file.
        """
        formattedResults = DBFormatter.formatDict(self, results)

        for formattedResult in formattedResults:
            if "file" in formattedResult.keys():
                formattedResult["file"] = int(formattedResult["file"])
            else:
                formattedResult["file"] = int(formattedResult["fileid"])

        return formattedResults

    def execute(self, subscription, run, conn = None, transaction = False):
        binds = {'subscription': subscription, 'run': run}
        results = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        return self.formatDict(results)
