#!/usr/bin/env python
"""
_Jobs_

MySQL implementation of Subscriptions.Jobs

Return a list of all jobs that exist for a subscription.
"""
__all__ = []
__revision__ = "$Id: Jobs.py,v 1.2 2008/10/15 07:56:36 jcgon Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class Jobs(DBFormatter):
    sql = "select id from wmbs_job where jobgroup in (select id from wmbs_jobgroup where subscription = :subscription)"

    def execute(self, subscription = 0, conn = None, transaction = False):
        binds = self.getBinds(subscription=subscription)
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return self.format(result)
