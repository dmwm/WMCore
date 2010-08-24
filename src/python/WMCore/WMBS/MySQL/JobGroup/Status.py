#!/usr/bin/env python
"""
_New_
MySQL implementation of JobGroup.Status
"""
__all__ = []
__revision__ = "$Id: Status.py,v 1.5 2008/12/10 23:01:46 sryu Exp $"
__version__ = "$Revision: 1.5 $"

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
    """

    def format(self, result):
        result = DBFormatter.format(self, result)
        return result[0]
        
    def execute(self, group=None, conn = None, transaction = False):
        binds = self.getBinds(jobgroup=group)
        self.logger.debug('JobGroup.Status sql: %s' % self.sql)
        self.logger.debug('JobGroup.Status binds: %s' % binds)
        result = self.dbi.processData(self.sql, binds)
        return self.format(result)