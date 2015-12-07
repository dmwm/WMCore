#!/usr/bin/env python
"""
_List_

MySQL implementation of Subscription.List
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class List(DBFormatter):
    sql = "SELECT id FROM wmbs_subscription"

    def format(self, result):
        results = DBFormatter.format(self, result)

        subIDs = []
        for row in results:
            subIDs.append(row[0])

        return subIDs

    def execute(self, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, conn = conn,
                                      transaction = transaction)
        return self.format(result)
