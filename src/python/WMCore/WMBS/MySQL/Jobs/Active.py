#!/usr/bin/env python
"""
_Active_
MySQL implementation of Jobs.Active

move file into wmbs_group_job_acquired
"""
__all__ = []
__revision__ = "$Id: Active.py,v 1.5 2009/01/21 22:00:27 sryu Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.Database.DBFormatter import DBFormatter

class Active(DBFormatter):
    sql = """insert into wmbs_group_job_acquired (job, jobgroup) 
             (select id, jobgroup from wmbs_job where id = :job)"""
    
    def format(self, result):
        return True
        
    def execute(self, job=0, conn = None, transaction = False):
        binds = self.getBinds(job=job)
        return self.format(self.dbi.processData(self.sql, binds, conn = conn,
                                                transaction = transaction))
