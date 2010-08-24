#!/usr/bin/env python
"""
_Failed_
MySQL implementation of Jobs.Failed

move file into wmbs_group_job_acquired
"""
__all__ = []
__revision__ = "$Id: Failed.py,v 1.1 2008/10/01 21:54:39 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class Failed(DBFormatter):
    sql = """insert into wmbs_group_job_failed (job, jobgroup) 
        select id, jobgroup from wmbs_job where id = :job"""
    
    def execute(self, job=0, conn = None, transaction = False):
        binds = self.getBinds(job=job)
        self.logger.debug('Job.Failed sql: %s' % self.sql)
        self.logger.debug('Job.Failed binds: %s' % binds)
        
        return self.format(self.dbi.processData(self.sql, binds))