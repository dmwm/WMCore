#!/usr/bin/env python
"""
_Exists_

MySQL implementation of BossLite.Jobs.Exists
"""

__all__ = []
__revision__ = "$Id: Exists.py,v 1.2 2010/05/10 12:57:39 spigafi Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class Exists(DBFormatter):
    """
    BossLite.Jobs.Exists
    """
    
    sql = """SELECT id FROM bl_job WHERE name = :name"""

    def execute(self, name, conn = None, transaction = False):
        """
        Expects unique name
        """
        
        result = self.dbi.processData(self.sql, {'name': name}, conn = conn,
                                      transaction = transaction)
        res = self.format(result)

        if res == []:
            return False
        else:
            return res[0][0]

        
