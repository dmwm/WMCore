#!/usr/bin/env python
"""
_New_
MySQL implementation of JobGroup.Status
"""
__all__ = []
__revision__ = "$Id: Status.py,v 1.1 2009/04/29 16:26:30 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class Status(DBFormatter):
    sql = """select (
        select count(job) from wmbs_group_job_acquired where job=:job
        ) as ac, (
        select count(job) from wmbs_group_job_failed where job=:job
        ) as fa, (
        select count(job) from wmbs_group_job_complete where job=:job
        ) as cm 
        from dual
    """

    def format(self, result):
        result = DBFormatter.format(self, result)
        return result[0]
        
    def execute(self, job, conn = None, transaction = False):
        binds = self.getBinds(job=job)
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        return self.format(result)
