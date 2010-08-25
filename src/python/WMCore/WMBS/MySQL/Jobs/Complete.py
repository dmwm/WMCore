#!/usr/bin/env python
"""
_Complete_

MySQL implementation of Jobs.Complete
"""

__all__ = []
__revision__ = "$Id: Complete.py,v 1.7 2009/03/20 14:29:19 sfoulkes Exp $"
__version__ = "$Revision: 1.7 $"

import time

from WMCore.Database.DBFormatter import DBFormatter

class Complete(DBFormatter):
    insertSQL = """INSERT INTO wmbs_group_job_complete (job, jobgroup)
                     SELECT :job, (SELECT jobgroup FROM wmbs_job WHERE id = :job)
                       FROM dual WHERE NOT EXISTS
                         (SELECT job FROM wmbs_group_job_complete WHERE job = :job)"""
    
    updateSQL = "UPDATE wmbs_job SET completion_time = %s WHERE id = :job" % int(time.time())
    
    def execute(self, job, conn = None, transaction = False):
        binds = {"job": job}
        self.dbi.processData(self.insertSQL, binds, conn = conn,
                             transaction = transaction)
        self.dbi.processData(self.updateSQL, binds, conn = conn,
                             transaction = transaction)        
        return
