#!/usr/bin/env python
"""
_GetAvailableFiles_

Oracle implementation of Subscription.GetAvailableFiles

Return a list of files that are available for processing.
Available means not acquired, complete or failed.
"""
__all__ = []
__revision__ = "$Id: GetAvailableFilesByLimit.py,v 1.1 2010/02/25 22:33:38 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFiles import \
     GetAvailableFiles as GetAvailableFiles

class GetAvailableFilesByLimit(GetAvailableFiles):
    
    def getSQLAndBinds(self, subscription, limit, conn = None, transaction = None):
        sql, bind = GetAvailableFiles.getSQLAndBinds(self, subscription, conn, transaction)
        
        sql = sql + " LIMIT :maxLimit"
        bind['maxLimit'] = limit
        return sql, bind
    
    def execute(self, subscription, limit, conn = None, transaction = False):
        sql, binds = self.getSQLAndBinds(subscription, limit, conn = conn,
                                         transaction = transaction)
        results = self.dbi.processData(sql, binds, conn = conn,
                                      transaction = transaction)
        return self.formatDict(results)