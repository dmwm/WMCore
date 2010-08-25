"""
_New_

MySQL implementation of Block.GetActiveBlocks
"""

__all__ = []
__revision__ = "$Id: GetActiveBlocks.py,v 1.1 2009/08/18 23:18:14 swakef Exp $"
__version__ = "$Revision: 1.1 $"

import time
from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WorkQueue.Database import States

class GetActiveBlocks(DBFormatter):
    sql = """SELECT wb.id, name, block_size, num_files, num_events
                FROM wq_block wb
                INNER JOIN wq_element we ON we.block_id = wb.id
                WHERE we.status = :available
          """

    def execute(self, conn = None, transaction = False):
        binds = {'available' : States['Available']}
        results = self.dbi.processData(self.sql, binds, conn = conn,
                                       transaction = transaction)
        return self.formatDict(results)
