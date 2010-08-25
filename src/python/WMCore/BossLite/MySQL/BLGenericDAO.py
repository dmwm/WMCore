#!/usr/bin/env python
"""
_BLGenericDAO_

MySQL implementation of BossLite.BLGenericDAO
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class BLGenericDAO(DBFormatter):

    def execute(self, rawSql, binds = {}, conn = None, transaction = False):
        """
        put your description here
        """
        
        result = self.dbi.processData(rawSql, binds, conn = conn,
                             transaction = transaction)
        
        return result
    