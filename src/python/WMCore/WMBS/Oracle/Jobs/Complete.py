#!/usr/bin/env python
"""
_Complete_
MySQL implementation of Jobs.Complete

move file into wmbs_group_job_acquired
"""
__all__ = []
__revision__ = "$Id: Complete.py,v 1.1 2008/10/08 14:30:09 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class Complete(MySQLBase):
    sql = """insert into wmbs_group_job_complete (job, jobgroup) 
        values (select id, jobgroup from wmbs_job where id = :job)"""
        
    def execute(self, job=0, conn = None, transaction = False):
        binds = self.getBinds(job=job)
        self.logger.debug('Job.Complete sql: %s' % self.sql)
        self.logger.debug('Job.Complete binds: %s' % binds)
        
        return self.format(self.dbi.processData(self.sql, binds))