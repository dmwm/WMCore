#!/usr/bin/env python
"""
_Delete_

MySQL implementation of Masks.Delete
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class Delete(DBFormatter):
    sql = "delete from wmbs_job_mask where job = :jobid"

    def execute(self, jobid, conn = None, transaction = False):
        binds = self.getBinds(jobid = jobid)
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
