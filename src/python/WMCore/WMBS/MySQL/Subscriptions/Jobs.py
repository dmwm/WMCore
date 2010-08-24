#!/usr/bin/env python
"""
_Jobs_

MySQL implementation of Subscriptions.Jobs
"""

__all__ = []
__revision__ = "$Id: Jobs.py,v 1.5 2009/01/16 22:40:03 sfoulkes Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.Database.DBFormatter import DBFormatter

class Jobs(DBFormatter):
    sql = """SELECT id FROM wmbs_job WHERE jobgroup IN
             (SELECT id FROM wmbs_jobgroup WHERE subscription = :subscription)"""

    def formatDict(self, results):
        """
        _formatDict_

        Cast the id column to an integer since formatDict() turns everything
        into strings.
        """
        formattedResults = DBFormatter.formatDict(self, results)

        for formattedResult in formattedResults:
            formattedResult["id"] = int(formattedResult["id"])

        return formatttedResults

    def execute(self, subscription = 0, conn = None, transaction = False):
        results = self.dbi.processData(self.sql, {"subscription": subscription},
                                       conn = conn, transaction = transaction)
        return self.formatDict(results)
