#!/usr/bin/env python
"""
_Failed_
SQLite implementation of Jobs.Failed

move file into wmbs_group_job_failed
"""

from WMCore.WMBS.MySQL.Jobs.Failed import Failed as FailedJobsMySQL

class Failed(FailedJobsMySQL):
    sql = FailedJobsMySQL.sql
        
    def execute(self, job=0, conn = None, transaction = False):
        FailedJobsMySQL.execute(self, job=job, conn=conn, transaction=transaction)
