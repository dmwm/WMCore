#!/usr/bin/env python
"""
_Jobs_

SQLite implementation of Subscriptions.Jobs
"""

__all__ = []



from WMCore.WMBS.MySQL.Subscriptions.Jobs import Jobs as JobsMySQL

class Jobs(JobsMySQL):
    sql = JobsMySQL.sql
