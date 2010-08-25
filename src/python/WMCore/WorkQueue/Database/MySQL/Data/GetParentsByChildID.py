"""
_New_

MySQL implementation of Block.GetParentByChildID
"""

__all__ = []
__revision__ = "$Id: GetParentsByChildID.py,v 1.1 2009/09/03 15:44:18 swakef Exp $"
__version__ = "$Revision: 1.1 $"

import time
from WMCore.Database.DBFormatter import DBFormatter

class GetParentsByChildID(DBFormatter):
    sql = """SELECT wb.id
                FROM wq_data wb
                INNER JOIN wq_data_parentage ON wb.id = parent
                WHERE child = :childID
          """

    def execute(self, childID, conn = None, transaction = False):
        binds = {"childID": childID}
        results = self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return self.formatDict(results)
