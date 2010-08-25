#!/usr/bin/env python
"""
_Complete_

MySQL implementation of Jobs.Complete
"""

__all__ = []
__revision__ = "$Id: Complete.py,v 1.8 2009/04/10 15:42:20 sryu Exp $"
__version__ = "$Revision: 1.8 $"

import time

from WMCore.Database.DBFormatter import DBFormatter

class Complete(DBFormatter):
    insertSQL = """INSERT INTO wmbs_group_job_complete (job, jobgroup)
                     SELECT :job, (SELECT jobgroup FROM wmbs_job WHERE id = :job)
                       FROM dual WHERE NOT EXISTS
                         (SELECT job FROM wmbs_group_job_complete WHERE job = :job)"""
    
    def execute(self, job, conn = None, transaction = False):
        binds = {"job": job}
        self.dbi.processData(self.insertSQL, binds, conn = conn,
                             transaction = transaction)       
        return
