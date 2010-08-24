#!/usr/bin/env python
"""
_GetAvailableFiles_

Oracle implementation of Subscription.GetAvailableFiles

Return a list of files that are available for processing.
Available means not acquired, complete or failed.
"""
__all__ = []



from WMCore.WMBS.Oracle.Subscriptions.GetFailedFiles import \
     GetFailedFiles

class GetFailedFilesByLimit(GetFailedFiles):
    
    sql = "SELECT * FROM (" + GetFailedFiles.sql + ") WHERE rownum <= :maxLimit"

    def execute(self, subscription, limit, conn = None, transaction = False):
        bind = {"subscription": subscription, 'maxLimit': limit}
        results = self.dbi.processData(self.sql, bind,
                                       conn = conn, transaction = transaction)
        return self.formatDict(results)