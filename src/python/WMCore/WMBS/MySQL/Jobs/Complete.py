#!/usr/bin/env python
"""
_Complete_
MySQL implementation of Jobs.Complete

move file into wmbs_group_job_complete
"""
__all__ = []
__revision__ = "$Id: Complete.py,v 1.6 2009/02/10 19:32:16 sryu Exp $"
__version__ = "$Revision: 1.6 $"

import time
from WMCore.Database.DBFormatter import DBFormatter

class Complete(DBFormatter):
    sql = ["""insert into wmbs_group_job_complete (job, jobgroup) VALUES
             (:job, (select jobgroup from wmbs_job where id = :job))""",
           " update wmbs_job set completion_time = %d where id = :job " 
           % time.time()]
    
    def execute(self, job=0, conn = None, transaction = False):
        binds = self.getBinds(job = job) * 2
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
