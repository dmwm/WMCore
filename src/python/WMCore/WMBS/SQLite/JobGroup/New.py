#!/usr/bin/env python
"""
_New_
SQLite implementation of JobGroup.New
"""
__all__ = []
__revision__ = "$Id: New.py,v 1.1 2008/10/16 15:34:12 jcgon Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.Services.UUID import makeUUID

class New(DBFormatter):
    sql = []
    sql.append("insert into wmbs_jobgroup (subscription, uid, last_update) values (:subscription, :uid, :timestamp)")
    sql.append("""select id from wmbs_jobgroup where uid=:uid""")
    def format(self, result, uid):
        result = DBFormatter.format(self, result)
        result = result[0][0]
        return result, uid
        
    def execute(self, subscription=None, conn = None, transaction = False):
        uid=makeUUID()
        binds = self.getBinds(subscription=subscription, uid=uid, timestamp=self.timestamp())
        self.logger.debug('JobGroup.New sql: %s' % self.sql)
        self.logger.debug('JobGroup.New binds: %s' % binds)
        result = self.dbi.processData(self.sql[0], binds)
        binds = self.getBinds(uid=uid)
        result = self.dbi.processData(self.sql[1], binds)
        return self.format(result, uid)
