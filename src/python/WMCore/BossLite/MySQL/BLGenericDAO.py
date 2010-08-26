#!/usr/bin/env python
"""
_BLGenericDAO_

MySQL implementation of BossLite.BLGenericDAO
"""

__all__ = []
__revision__ = "$Id: BLGenericDAO.py,v 1.1 2010/05/04 15:36:19 spigafi Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class BLGenericDAO(DBFormatter):

    def execute(self, rawSql, binds = {}, conn = None, transaction = False):
        """
        put your description here
        """
        
        result = self.dbi.processData(rawSql, binds, conn = conn,
                             transaction = transaction)
        
        return result
    