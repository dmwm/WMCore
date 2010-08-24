#!/usr/bin/env python
"""
_Delete_

MySQL implementation of Masks.Delete
"""

__all__ = []
__revision__ = "$Id: Delete.py,v 1.2 2009/01/11 17:44:41 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class Delete(DBFormatter):
    sql = "delete from wmbs_job_mask where job = :jobid"
    
    def execute(self, jobid, conn = None, transaction = False):
        binds = self.getBinds(jobid = jobid)
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
