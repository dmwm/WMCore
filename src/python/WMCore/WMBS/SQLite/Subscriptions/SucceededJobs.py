#!/usr/bin/env python
"""
_SucceededJobs_

SQLite implementation of Subscriptions.SucceededJobs
"""

__all__ = []



from WMCore.WMBS.MySQL.Subscriptions.SucceededJobs import SucceededJobs as SucceededJobsMySQL

class SucceededJobs(SucceededJobsMySQL):
    sql = SucceededJobsMySQL.sql


