#!/usr/bin/env python
"""
_New_

MySQL implementation of Jobs.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.6 2008/11/25 17:21:43 sfoulkes Exp $"
__version__ = "$Revision: 1.6 $"

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = ["insert into wmbs_job (jobgroup, name) values (:jobgroup, :name)",
           """insert into wmbs_job_mask (job) select id from wmbs_job
              where jobgroup = :jobgroup and name = :name""",
           "select id from wmbs_job where jobgroup = :jobgroup and name = :name"]
    
    def format(self, result):
        result = DBFormatter.format(self, result)
        return result[0][0]
        
    def execute(self, jobgroup, name):
        binds = self.getBinds(jobgroup=jobgroup, name=name) * 3
        
        return self.format(self.dbi.processData(self.sql, binds))
