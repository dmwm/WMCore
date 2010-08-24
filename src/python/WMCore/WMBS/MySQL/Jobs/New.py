#!/usr/bin/env python
"""
_New_
MySQL implementation of Jobs.New
"""
__all__ = []
__revision__ = "$Id: New.py,v 1.1 2008/07/03 17:03:59 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Base import MySQLBase

class New(MySQLBase):
    sql = []
    sql.append("insert into wmbs_job (subscription) values (:subscription)")
    sql.append("select id, last_update from wmbs_job where id = (select LAST_INSERT_ID()) and subscription = :subscription")
                
    def getBinds(self, subscription=0):
        # Can't use self.dbi.buildbinds here...
        binds = [{'subscription':subscription},
                 {'subscription':subscription}]
        return binds
    
    def format(self, result):
        result = MySQLBase.format(self, result)
        return result[0]
        
    def execute(self, subscription=0, conn = None, transaction = False):
        binds = self.getBinds(subscription)
        self.logger.debug('Job.Add sql: %s' % self.sql)
        self.logger.debug('Job.Add binds: %s' % binds)
        result = self.dbi.processData(self.sql, binds)
        
        return self.format(result)