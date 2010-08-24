#!/usr/bin/env python
"""
_New_

MySQL implementation of Masks.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.1 2008/11/20 17:20:48 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = "insert into wmbs_job_mask (job) values (:jobid)"
    
    def execute(self, jobid):
        binds = self.getBinds(jobid = jobid)
        self.dbi.processData(self.sql, binds)
        return
