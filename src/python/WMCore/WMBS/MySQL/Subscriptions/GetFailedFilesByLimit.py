#!/usr/bin/env python
"""
_GetAvailableFiles_

Oracle implementation of Subscription.GetAvailableFiles

Return a list of files that are available for processing.
Available means not acquired, complete or failed.
"""
__all__ = []
__revision__ = "$Id: GetFailedFilesByLimit.py,v 1.1 2010/02/26 20:21:06 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Subscriptions.GetFailedFiles import \
     GetFailedFiles

class GetFailedFilesByLimit(GetFailedFiles):
    
    sql = GetFailedFiles.sql + " LIMIT :maxLimit"

    def execute(self, subscription, limit, conn = None, transaction = False):
        bind = {"subscription": subscription, 'maxLimit': limit}
        results = self.dbi.processData(self.sql, bind,
                                       conn = conn, transaction = transaction)
        return self.formatDict(results)