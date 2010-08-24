#!/usr/bin/env python
"""
_New_
Oracle implementation of JobGroup.New
"""
__all__ = []
__revision__ = "$Id: New.py,v 1.1 2008/11/24 21:51:45 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.JobGroup.New import New as NewJobGroupMySQL

class New(NewJobGroupMySQL):
    """
    _New_
    Oracle specific sql: 'uid' is a reserved word in Oracle 
    """
    sql = []
    sql.append("""insert into wmbs_jobgroup (id, subscription, guid, output, last_update) 
                  values (wmbs_jobgroup_SEQ.nextval, :subscription, 
                          :guid, :output, :timestamp)""")
    sql.append("""select id from wmbs_jobgroup where guid=:guid""")
    
    def execute(self, uid, subscription=None, output=None, conn = None, transaction = False):
        binds = self.getBinds(subscription=subscription, guid=uid, output=output, 
                              timestamp=self.timestamp())
        self.logger.debug('JobGroup.New sql: %s' % self.sql)
        self.logger.debug('JobGroup.New binds: %s' % binds)
        result = self.dbi.processData(self.sql[0], binds)
        binds = self.getBinds(guid=uid)
        result = self.dbi.processData(self.sql[1], binds)
        return self.format(result, uid)