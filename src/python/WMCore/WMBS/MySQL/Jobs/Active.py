#!/usr/bin/env python
"""
_Active_
MySQL implementation of Jobs.Active

move file into wmbs_group_job_acquired
"""
__all__ = []
__revision__ = "$Id: Active.py,v 1.6 2009/01/27 16:47:15 sfoulkes Exp $"
__version__ = "$Revision: 1.6 $"

from WMCore.Database.DBFormatter import DBFormatter

class Active(DBFormatter):
    sql = """insert into wmbs_group_job_acquired (job, jobgroup) VALUES
             (:job, (select jobgroup from wmbs_job where id = :job))"""
    
    def execute(self, job=0, conn = None, transaction = False):
        binds = self.getBinds(job=job)
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)        
        return 
