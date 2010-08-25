#!/usr/bin/env python
"""
_New_
MySQL implementation of JobGroup.Status
"""
__all__ = []
__revision__ = "$Id: Status.py,v 1.10 2009/04/23 00:02:12 sryu Exp $"
__version__ = "$Revision: 1.10 $"

from WMCore.Database.DBFormatter import DBFormatter

class Status(DBFormatter):
    sql = """select (
        select count(id) from wmbs_job wj
            left outer join wmbs_group_job_acquired wa on wj.id = wa.job
            left outer join wmbs_group_job_failed wf on wj.id = wf.job
            left outer join wmbs_group_job_complete wc on wj.id = wc.job
            where jobgroup=:jobgroup and wa.job is null
                  and wf.job is null and wc.job is null
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
