#!/usr/bin/env python
"""
_Jobs_

Oracle implementation of Subscriptions.Jobs

Return a list of all jobs that exist for a subscription.
"""

__all__ = []



from WMCore.WMBS.MySQL.Subscriptions.Jobs import Jobs as JobsMySQL

class Jobs(JobsMySQL):
    sql = JobsMySQL.sql
