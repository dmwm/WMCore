"""
_New_

MySQL implementation of Block.GetParentByChildID
"""

__all__ = []
__revision__ = "$Id: GetParentsByChildID.py,v 1.3 2009/08/18 23:18:14 swakef Exp $"
__version__ = "$Revision: 1.3 $"

import time
from WMCore.Database.DBFormatter import DBFormatter

class GetParentsByChildID(DBFormatter):
    sql = """SELECT wb.id, name, block_size, num_files, num_events    
                FROM wq_block wb
                INNER JOIN wq_block_parentage ON wb.id = parent  
                WHERE child = :childID    
          """

    def execute(self, childID, conn = None, transaction = False):
        binds = {"childID": childID}
        results = self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)            
        return self.formatDict(results)
