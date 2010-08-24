#!/usr/bin/env python
"""
_LoadSubscription_

MySQL implementation of JobGroup.LoadSubscription
"""

__all__ = []
__revision__ = "$Id: LoadSubscription.py,v 1.2 2009/01/12 19:26:03 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class LoadSubscription(DBFormatter):
    sql = "select subscription from wmbs_jobgroup where id = :id"

    def format(self, result):
        result = DBFormatter.format(self, result)
        return result[0][0]

    def execute(self, id, conn = None, transaction = False):
        binds = self.getBinds(id=id)
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        return self.format(result)
