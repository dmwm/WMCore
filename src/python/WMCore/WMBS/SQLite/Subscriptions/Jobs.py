#!/usr/bin/env python
"""
_Jobs_

SQLite implementation of Subscriptions.Jobs

Return a list of all jobs that exist for a subscription.
"""
__all__ = []
__revision__ = "$Id: Jobs.py,v 1.2 2008/10/16 15:38:11 jcgon Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Subscriptions.Jobs import Jobs as JobsMySQL
#class Jobs(DBFormatter, JobsMySQL):
class Jobs(SQLiteBase, JobsMySQL):
    sql = JobsMySQL.sql

    def execute(self, subscription = 0, conn = None, transaction = False):
        binds = self.getBinds(subscription=subscription)
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return self.format(result)
