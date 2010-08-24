#!/usr/bin/env python
"""
_Active_
MySQL implementation of Jobs.Active

move file into wmbs_group_job_acquired
"""
__all__ = []
__revision__ = "$Id: Active.py,v 1.3 2008/11/24 21:47:05 sryu Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter

class Active(DBFormatter):
    sql = """insert into wmbs_group_job_acquired (job, jobgroup) 
             values (select id, jobgroup from wmbs_job where id = :job)"""
    
    def format(self, result):
        return True
        
    def execute(self, job=0, conn = None, transaction = False):
        binds = self.getBinds(job=job)
        self.logger.debug('Job.Active sql: %s' % self.sql)
        self.logger.debug('Job.Active binds: %s' % binds)
        
        return self.format(self.dbi.processData(self.sql, binds))
