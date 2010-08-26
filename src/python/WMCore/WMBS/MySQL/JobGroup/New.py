#!/usr/bin/env python
"""
_New_

MySQL implementation of JobGroup.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.8 2009/01/11 17:46:39 sfoulkes Exp $"
__version__ = "$Revision: 1.8 $"

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = """INSERT INTO wmbs_jobgroup (subscription, uid, output,
             last_update) VALUES (:subscription, :guid, :output,
             unix_timestamp())"""

    def execute(self, uid, subscription = None, output = None, conn = None,
                transaction = False):
        binds = self.getBinds(subscription = subscription, guid = uid,
                              output = output)
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        return
