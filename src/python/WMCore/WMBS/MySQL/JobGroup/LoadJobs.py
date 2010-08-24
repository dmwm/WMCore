#!/usr/bin/env python
"""
_LoadJobs_

MySQL implementation of JobGroup.LoadJobs
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class LoadJobs(DBFormatter):
    sql = "SELECT id FROM wmbs_job WHERE jobgroup = :jobgroup"

    def formatDict(self, results):
        """
        _formatDict_

        Cast the id to be an integer as formatDict() turns all results into
        strings.
        """
        formattedResults = DBFormatter.formatDict(self, results)

        for formattedResult in formattedResults:
            formattedResult["id"] = int(formattedResult["id"])

        return formattedResults

    def execute(self, jobgroup, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"jobgroup": jobgroup},
                                      conn = conn, transaction = transaction)
        return self.formatDict(result)
