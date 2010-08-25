#!/usr/bin/env python
"""
_GetCompletedFilesByRun_

MySQL implementation of Subscription.GetCompletedFiles
"""

__all__ = []
__revision__ = "$Id: GetCompletedFilesByRun.py,v 1.1 2009/05/01 19:41:31 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetCompletedFilesByRun(DBFormatter):
    sql = """SELECT wc.file FROM wmbs_sub_files_complete wc
               INNER JOIN wmbs_file_runlumi_map wm ON (wm.file = wc.file)
             WHERE wc.subscription = :subscription AND run = :run
             """

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
    
    def execute(self, subscription, run, conn = None,
                transaction = False):
        binds = {"subscription": subscription, "run": run}
        results = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)
        return self.formatDict(results)
