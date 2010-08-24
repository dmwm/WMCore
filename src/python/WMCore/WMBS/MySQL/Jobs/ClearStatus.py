#!/usr/bin/env python
"""
_ClearStatus_
MySQL implementation of Jobs.ClearStatus

Delete all status information. For resubmissions and for each state change.
"""
__all__ = []
__revision__ = "$Id: ClearStatus.py,v 1.2 2008/12/10 22:26:15 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class ClearStatus(DBFormatter):
    sql = ["delete from wmbs_group_job_acquired where job = :job", 
    "delete from wmbs_group_job_failed where job = :job", 
    "delete from wmbs_group_job_complete where job = :job"]
    
    def format(self, result):
        return True
    
    def execute(self, job=0, conn = None, transaction = False):
        binds = self.getBinds(job=job) * 3
        self.logger.debug('Job.ClearStatus sql: %s' % self.sql)
        self.logger.debug('Job.ClearStatus binds: %s' % binds)
        
        return self.format(self.dbi.processData(self.sql, binds))