#!/usr/bin/env python
"""
_Complete_
MySQL implementation of Jobs.Complete

move file into wmbs_group_job_acquired
"""
__all__ = []
__revision__ = "$Id: Complete.py,v 1.3 2008/12/10 22:26:15 sryu Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter

class Complete(DBFormatter):
    sql = """insert into wmbs_group_job_complete (job, jobgroup) 
             (select id, jobgroup from wmbs_job where id = :job)"""
    
    def format(self, result):
        return True
    
    def execute(self, job=0, conn = None, transaction = False):
        binds = self.getBinds(job=job)
        self.logger.debug('Job.Complete sql: %s' % self.sql)
        self.logger.debug('Job.Complete binds: %s' % binds)
        
        return self.format(self.dbi.processData(self.sql, binds))