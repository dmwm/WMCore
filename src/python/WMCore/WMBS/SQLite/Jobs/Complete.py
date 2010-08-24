#!/usr/bin/env python
"""
_Complete_
SQLite implementation of Jobs.Complete

move file into wmbs_group_job_completed
"""

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Jobs.Complete import Complete as CompleteJobsMySQL

class Complete(SQLiteBase, CompleteJobsMySQL):
    sql = CompleteJobsMySQL.sql
        
    def execute(self, job=0, conn = None, transaction = False):
        CompleteJobsMySQL.execute(self, job=job, conn=conn, transaction=transaction)
