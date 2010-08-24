#!/usr/bin/env python
"""
_ClearStatus_
MySQL implementation of Jobs.ClearStatus

Delete all status information. For resubmissions and for each state change.
"""
__all__ = []
__revision__ = "$Id: ClearStatus.py,v 1.3 2009/01/12 19:26:03 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter

class ClearStatus(DBFormatter):
    sql = ["delete from wmbs_group_job_acquired where job = :job", 
    "delete from wmbs_group_job_failed where job = :job", 
    "delete from wmbs_group_job_complete where job = :job"]
    
    def format(self, result):
        return True
    
    def execute(self, job=0, conn = None, transaction = False):
        binds = self.getBinds(job=job) * 3
        return self.format(self.dbi.processData(self.sql, binds, conn = conn,
                                                transaction = transaction))
