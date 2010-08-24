#!/usr/bin/env python
"""
_Jobs_

MySQL implementation of Subscriptions.Jobs

Return a list of all jobs that exist for a subscription.
"""

__all__ = []
__revision__ = "$Id: Jobs.py,v 1.3 2009/01/11 17:41:59 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter

class Jobs(DBFormatter):
    sql = """SELECT id FROM wmbs_job WHERE jobgroup IN
             (SELECT id FROM wmbs_jobgroup WHERE subscription = :subscription)"""

    def execute(self, subscription = 0, conn = None, transaction = False):
        binds = self.getBinds(subscription=subscription)
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return self.format(result)
