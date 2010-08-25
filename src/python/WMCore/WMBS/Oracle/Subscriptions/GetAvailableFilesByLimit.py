#!/usr/bin/env python
"""
_GetAvailableFilesByLimit_

Oracle implementation of Subscription.GetAvailableFilesByLimit
"""




from WMCore.WMBS.Oracle.Subscriptions.GetAvailableFiles import GetAvailableFiles as GetAvailableFilesOracle

class GetAvailableFilesByLimit(GetAvailableFilesOracle):
    def execute(self, subscription, limit, conn = None, transaction = False):
        self.sql = "SELECT * FROM (" + self.sql + ") WHERE rownum <= :maxLimit"        
        results = self.dbi.processData(self.sql, {"subscription": subscription,
                                                  "maxLimit": limit},
                                       conn = conn, transaction = transaction)
        return self.formatDict(results)
