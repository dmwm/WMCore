#!/usr/bin/env python
"""
_LoadFromID_

MySQL implementation of JobGroup.LoadFromID
"""

__all__ = []
__revision__ = "$Id: LoadFromID.py,v 1.1 2009/01/14 16:35:26 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class LoadFromID(DBFormatter):
    sql = """SELECT id, subscription, uid, output, last_update
             FROM wmbs_jobgroup WHERE id = :groupid"""

    def execute(self, id, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"groupid": id}, conn = conn,
                                      transaction = transaction)
        return self.formatDict(result)[0]
