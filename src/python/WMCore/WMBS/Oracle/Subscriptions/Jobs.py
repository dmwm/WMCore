#!/usr/bin/env python
"""
_Jobs_

SQLite implementation of Subscriptions.Jobs

Return a list of all jobs that exist for a subscription.
"""
__all__ = []
__revision__ = "$Id: Jobs.py,v 1.2 2008/11/24 21:51:48 sryu Exp $"
__version__ = "$Revision: 1.2 $"


from WMCore.WMBS.MySQL.Subscriptions.Jobs import Jobs as JobsMySQL

class Jobs(JobsMySQL):
    sql = JobsMySQL.sql