#!/usr/bin/env python
"""
_New_
MySQL implementation of JobGroup.Status
"""
__all__ = []
__revision__ = "$Id: Status.py,v 1.9 2009/04/22 23:28:53 sryu Exp $"
__version__ = "$Revision: 1.9 $"

from WMCore.Database.DBFormatter import DBFormatter

class Status(DBFormatter):
    sql = """select (
        select count(id) from wmbs_job where jobgroup=:jobgroup 
            and id not in (select job from wmbs_group_job_acquired) 
            and id not in (select job from wmbs_group_job_failed)
            and id not in (select job from wmbs_group_job_complete)
        ) as av, (
        select count(job) from wmbs_group_job_acquired where jobgroup=:jobgroup
        ) as ac, (
        select count(job) from wmbs_group_job_failed where jobgroup=:jobgroup
        ) as fa, (
        select count(job) from wmbs_group_job_complete where jobgroup=:jobgroup
        ) as cm 
        from dual
    """

    def format(self, result):
        result = DBFormatter.format(self, result)
        return result[0]
        
    def execute(self, group=None, conn = None, transaction = False):
        binds = self.getBinds(jobgroup=group)
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        return self.format(result)
