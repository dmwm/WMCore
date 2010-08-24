#!/usr/bin/env python
"""
_Active_
MySQL implementation of Jobs.Active

move file into wmbs_group_job_acquired
"""

from WMCore.WMBS.MySQL.Jobs.Active import Active as ActiveJobsMySQL
class Active(ActiveJobsMySQL):
    sql = ActiveJobsMySQL.sql

    def execute(self, job=0, conn = None, transaction = False):
        ActiveJobsMySQL.execute(self, job=job, conn=conn, transaction=transaction)
