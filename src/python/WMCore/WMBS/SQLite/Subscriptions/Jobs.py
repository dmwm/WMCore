#!/usr/bin/env python
"""
_Jobs_

SQLite implementation of Subscriptions.Jobs

Return a list of all jobs that exist for a subscription.
"""
__all__ = []
__revision__ = "$Id: Jobs.py,v 1.1 2008/08/09 22:15:46 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WMBS.MySQL.Subscriptions.Jobs import Jobs as JobsMySQL
class Jobs(DBFormatter, JobsMySQL):
    sql = JobsMySQL.sql