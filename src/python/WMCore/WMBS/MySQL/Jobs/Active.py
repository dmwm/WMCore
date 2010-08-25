#!/usr/bin/env python
"""
_Active_

MySQL implementation of Jobs.Active
"""

__all__ = []
__revision__ = "$Id: Active.py,v 1.8 2009/04/10 15:42:20 sryu Exp $"
__version__ = "$Revision: 1.8 $"
import time

from WMCore.Database.DBFormatter import DBFormatter

class Active(DBFormatter):
    insertSQL = """INSERT INTO wmbs_group_job_acquired (job, jobgroup)
                     SELECT :job, (SELECT jobgroup FROM wmbs_job WHERE id = :job)
                       FROM dual WHERE NOT EXISTS
                         (SELECT job FROM wmbs_group_job_acquired WHERE job = :job)"""    
    
    updateSQL = "UPDATE wmbs_job SET submission_time = %s WHERE id = :job" % int(time.time())
    
    def execute(self, job, conn = None, transaction = False):
        binds = {"job": job}
        self.dbi.processData(self.insertSQL, binds, conn = conn,
                             transaction = transaction)
        self.dbi.processData(self.updateSQL, binds, conn = conn,
                             transaction = transaction)         
        return 
