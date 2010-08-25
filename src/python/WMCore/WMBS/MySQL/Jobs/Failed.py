#!/usr/bin/env python
"""
_Failed_

MySQL implementation of Jobs.Failed
"""

__all__ = []
__revision__ = "$Id: Failed.py,v 1.9 2009/04/27 21:12:12 sryu Exp $"
__version__ = "$Revision: 1.9 $"

import time

from WMCore.Database.DBFormatter import DBFormatter

class Failed(DBFormatter):
    insertSQL = """INSERT INTO wmbs_group_job_failed (job, jobgroup)
                     SELECT :job, (SELECT jobgroup FROM wmbs_job WHERE id = :job)
                       FROM dual WHERE NOT EXISTS
                         (SELECT job FROM wmbs_group_job_failed WHERE job = :job)"""
    
    updateSQL = "UPDATE wmbs_job SET completion_time = :time WHERE id = :job"
    
    def execute(self, job, conn = None, transaction = False):
        binds = {"job": job}
        self.dbi.processData(self.insertSQL, binds, conn = conn,
                             transaction = transaction)
        binds["time"] = int(time.time())
        self.dbi.processData(self.updateSQL, binds, conn = conn,
                             transaction = transaction)
        return
