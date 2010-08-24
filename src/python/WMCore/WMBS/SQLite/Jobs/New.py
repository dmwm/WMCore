#!/usr/bin/env python
"""
_New_
SQLite implementation of Jobs.New
"""
__all__ = []
__revision__ = "$Id: New.py,v 1.2 2008/10/16 15:30:02 jcgon Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = []
    sql.append("insert into wmbs_job (jobgroup, name, last_update) values (:jobgroup, :name, :timestamp)")
    sql.append("select id from wmbs_job where jobgroup = :jobgroup and name = :name")        
    #sql = ["insert into wmbs_job (jobgroup, name, last_update) values (:jobgroup, :name, :timestamp)",
    #"select id from wmbs_job where jobgroup = :jobgroup and name = :name"]
    
    def format(self, result):
        result = DBFormatter.format(self, result)
        return result[0][0]
        
    def execute(self, jobgroup=0, name=None, conn = None, transaction = False):
        binds = self.getBinds(jobgroup=jobgroup, name=name, timestamp=self.timestamp())
        self.logger.debug('Job.Add sql: %s' % self.sql[0])
        self.logger.debug('Job.Add binds: %s' % binds)
        result = self.dbi.processData(self.sql[0], binds)

        binds = self.getBinds(jobgroup=jobgroup, name=name)
        self.logger.debug('Job.Add sql: %s' % self.sql[1])
        self.logger.debug('Job.Add binds: %s' % binds)
        result = self.dbi.processData(self.sql[1], binds)

        return self.format(result)
