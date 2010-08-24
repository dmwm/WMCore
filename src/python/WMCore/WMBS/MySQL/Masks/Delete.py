#!/usr/bin/env python
"""
_Delete_

MySQL implementation of Masks.Delete
"""

__all__ = []
__revision__ = "$Id: Delete.py,v 1.1 2008/11/20 17:20:48 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class Delete(DBFormatter):
    sql = "delete from wmbs_job_mask where job = :jobid"
    
    def execute(self, jobid):
        binds = self.getBinds(jobid = jobid)
        self.dbi.processData(self.sql, binds)
        return
