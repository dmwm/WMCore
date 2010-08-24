#!/usr/bin/env python
"""
_GetAvailableFilesByLimit_

MySQL implementation of Subscription.GetAvailableFilesByLimit
"""




from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFiles import GetAvailableFiles as GetAvailableFilesMySQL

class GetAvailableFilesByLimit(GetAvailableFilesMySQL):
    def execute(self, subscription, limit, conn = None, transaction = False):
        self.sql += " LIMIT :maxLimit"
        
        results = self.dbi.processData(self.sql, {"subscription": subscription,
                                                  "maxLimit": limit},
                                       conn = conn, transaction = transaction)
        return self.formatDict(results)
