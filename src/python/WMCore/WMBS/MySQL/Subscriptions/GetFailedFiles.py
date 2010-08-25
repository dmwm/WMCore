#!/usr/bin/env python
"""
_GetFailedFiles_

MySQL implementation of Subscription.GetFailedFiles
"""

__all__ = []
__revision__ = "$Id: GetFailedFiles.py,v 1.7 2009/03/16 16:58:39 sfoulkes Exp $"
__version__ = "$Revision: 1.7 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetFailedFiles(DBFormatter):
    sql = """SELECT file FROM wmbs_sub_files_failed
             WHERE subscription = :subscription
             LIMIT :maxfiles
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
    
    def execute(self, subscription = None, maxFiles = 100, conn = None,
                transaction = False):
        results = self.dbi.processData(self.sql, {"subscription": subscription,
                                                  "maxfiles": maxFiles},
                         conn = conn, transaction = transaction)
        return self.formatDict(results)
