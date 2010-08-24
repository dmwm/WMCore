#!/usr/bin/env python
"""
_Jobs_

MySQL implementation of Subscriptions.Jobs

Return a list of all jobs that exist for a subscription.
"""
__all__ = []
__revision__ = "$Id: Jobs.py,v 1.1 2008/08/09 22:15:45 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class Jobs(DBFormatter):
    sql = "select id from wmbs_job where subscription = :subscription"
    
    def execute(self, subscription = 0, conn = None, transaction = False):
        binds = self.getBinds(subscription=subscription)
        result = self.dbi.processData(sql, binds, 
                         conn = conn, transaction = transaction)
        return self.format(result)