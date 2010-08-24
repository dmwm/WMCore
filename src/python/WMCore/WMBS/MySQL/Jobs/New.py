#!/usr/bin/env python
"""
_New_
MySQL implementation of Jobs.New
"""
__all__ = []
__revision__ = "$Id: New.py,v 1.4 2008/10/01 21:43:16 metson Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = ["insert into wmbs_job (jobgroup, name) values (:jobgroup, :name)",
    "select id from wmbs_job where jobgroup = :jobgroup and name = :name"]
    
    def format(self, result):
        result = DBFormatter.format(self, result)
        return result[0][0]
        
    def execute(self, jobgroup=0, name=None, conn = None, transaction = False):
        binds = self.getBinds(jobgroup=jobgroup, name=name) * 2
        self.logger.debug('Job.Add sql: %s' % self.sql)
        self.logger.debug('Job.Add binds: %s' % binds)
        
        return self.format(self.dbi.processData(self.sql, binds))