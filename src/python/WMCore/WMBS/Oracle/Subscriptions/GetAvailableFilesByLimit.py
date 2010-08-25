#!/usr/bin/env python
"""
_GetAvailableFilesByLimit_

Oracle implementation of Subscription.GetAvailableFilesByLimit

Return a list of files that are available for processing.
Available means not acquired, complete or failed.
"""
__all__ = []
__revision__ = "$Id: GetAvailableFilesByLimit.py,v 1.1 2010/02/25 22:33:23 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.Oracle.Subscriptions.GetAvailableFiles import GetAvailableFiles 

class GetAvailableFilesByLimit(GetAvailableFiles):
    
    def getSQLAndBinds(self, subscription, limit, conn = None, transaction = None):
        sql, bind = GetAvailableFiles.getSQLAndBinds(self, subscription, conn, transaction)
        
        sql = "SELECT * FROM (" + sql + ") WHERE rownum <= :maxLimit"
        bind['maxLimit'] = limit
        return sql, bind
    
    def execute(self, subscription, limit, conn = None, transaction = False):
        sql, binds = self.getSQLAndBinds(subscription, limit, conn = conn,
                                         transaction = transaction)
        results = self.dbi.processData(sql, binds, conn = conn,
                                      transaction = transaction)
        return self.formatDict(results)