#!/usr/bin/env python
"""
_GetFailedFiles_

MySQL implementation of Subscription.GetFailedFiles
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class GetFailedFilesByRun(DBFormatter):
    sql = """SELECT wf.file FROM wmbs_sub_files_failed wf
               INNER JOIN wmbs_file_runlumi_map wm ON (wm.file = wf.file)
             WHERE wf.subscription = :subscription AND run = :run
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

