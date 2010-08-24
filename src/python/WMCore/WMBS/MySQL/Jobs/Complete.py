#!/usr/bin/env python
"""
_Complete_
MySQL implementation of Jobs.Complete

move file into wmbs_group_job_acquired
"""
__all__ = []
__revision__ = "$Id: Complete.py,v 1.5 2009/01/27 16:47:15 sfoulkes Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.Database.DBFormatter import DBFormatter

class Complete(DBFormatter):
    sql = """insert into wmbs_group_job_complete (job, jobgroup) VALUES
             (:job, (select jobgroup from wmbs_job where id = :job))"""
    
    def execute(self, job=0, conn = None, transaction = False):
        binds = self.getBinds(job = job)
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
