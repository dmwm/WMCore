#!/usr/bin/env python
"""
_Create_

MySQL implementation of BossLite.Jobs.Exists
"""

__all__ = []
__revision__ = "$Id: Exists.py,v 1.1 2010/03/30 10:19:00 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class Exists(DBFormatter):
    sql = """SELECT id FROM bl_task WHERE name = :name"""


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

        
