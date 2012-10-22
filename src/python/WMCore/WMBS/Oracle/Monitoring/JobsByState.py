#!/usr/bin/env python
"""
_JobsByState_

Monitoring DAO classes for Jobs in WMBS
"""
__all__ = []



from WMCore.WMBS.MySQL.Monitoring.JobsByState import JobsByState \
    as JobsByStateMySQL

class JobsByState(JobsByStateMySQL):

    #TO check what else is needed for return item
    sql = JobsByStateMySQL.sql
