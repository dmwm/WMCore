#!/usr/bin/env python
"""
_ListIncomplete_

MySQL implementation of Subscription.ListIncomplete
"""

from WMCore.Database.DBFormatter import DBFormatter

class ListIncomplete(DBFormatter):
    sql = "SELECT DISTINCT subscription AS id FROM wmbs_sub_files_available"

    def format(self, result):
        results = DBFormatter.format(self, result)

        subIDs = []
        for row in results:
            subIDs.append(row[0])

        return subIDs

    def execute(self, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, conn = conn, transaction = transaction)
        return self.format(result)
