#!/usr/bin/env python
"""
_LoadFromID_

MySQL implementation of Subscription.LoadFromID
"""

__all__ = []
__revision__ = "$Id: LoadFromID.py,v 1.1 2009/01/14 16:35:25 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class LoadFromID(DBFormatter):
    sql = """SELECT id, fileset, workflow, split_algo, type, last_update
             FROM wmbs_subscription WHERE id = :id"""
    
    def execute(self, id = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"id": id}, conn = conn,
                                      transaction = transaction)
        return self.formatDict(result)[0]
