#!/usr/bin/env python
"""
_New_

MySQL implementation of Jobs.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.5 2008/11/20 17:16:53 sfoulkes Exp $"
__version__ = "$Revision: 1.5 $"

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
        self.logger.debug('Job.Add sql: %s' % self.sql)
        self.logger.debug('Job.Add binds: %s' % binds)
        
        return self.format(self.dbi.processData(self.sql, binds))
