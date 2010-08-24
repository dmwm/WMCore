#!/usr/bin/env python
"""
_Failed_
MySQL implementation of Jobs.Failed

move file into wmbs_group_job_acquired
"""
__all__ = []
__revision__ = "$Id: Failed.py,v 1.6 2009/02/13 21:17:22 sryu Exp $"
__version__ = "$Revision: 1.6 $"
import time
from WMCore.Database.DBFormatter import DBFormatter

class Failed(DBFormatter):
    sql = ["""insert into wmbs_group_job_failed (job, jobgroup) VALUES
             (:job, (select jobgroup from wmbs_job where id = :job))""",
           " update wmbs_job set completion_time = %d where id = :job " 
           % time.time()]
    
    def execute(self, job=0, conn = None, transaction = False):
        binds = self.getBinds(job=job) * 2
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
