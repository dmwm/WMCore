#!/usr/bin/env python
"""
_Complete_
MySQL implementation of Jobs.Complete

move file into wmbs_group_job_acquired
"""
__all__ = []
__revision__ = "$Id: Complete.py,v 1.2 2008/11/20 21:52:32 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class Complete(DBFormatter):
    sql = """insert into wmbs_group_job_complete (job, jobgroup) 
             values (select id, jobgroup from wmbs_job where id = :job)"""
    
    def execute(self, job=0, conn = None, transaction = False):
        binds = self.getBinds(job=job)
        self.logger.debug('Job.Complete sql: %s' % self.sql)
        self.logger.debug('Job.Complete binds: %s' % binds)
        
        return self.format(self.dbi.processData(self.sql, binds))