#!/usr/bin/env python
"""
_New_

MySQL implementation of Masks.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.2 2008/11/24 21:47:07 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    
    sql = "insert into wmbs_job_mask (job, inclusivemask) values (:jobid, :inclusivemask)"
    
    def format(self,result):
        return True
    
    def execute(self, jobid, inclusivemask=None):
        if inclusivemask == None:
            binds = self.getBinds(jobid = jobid, inclusivemask=True)
        else:
            binds = self.getBinds(jobid = jobid, inclusivemask = inclusivemask)
        result = self.dbi.processData(self.sql, binds)
        return self.format(result)
