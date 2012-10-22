#!/usr/bin/env python
"""
_JobsByState_

Monitoring DAO classes for Jobs in WMBS
"""
__all__ = []



from WMCore.WMBS.MySQL.Monitoring.JobCountByState import JobCountByState \
 as JobCountByStateMySQL

class JobCountByState(JobCountByStateMySQL):
    sql = JobCountByStateMySQL.sql
