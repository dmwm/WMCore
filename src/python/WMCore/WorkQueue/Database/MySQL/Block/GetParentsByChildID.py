"""
_New_

MySQL implementation of Block.GetParentByChildID
"""

__all__ = []
__revision__ = "$Id: GetParentsByChildID.py,v 1.1 2009/06/15 20:56:57 sryu Exp $"
__version__ = "$Revision: 1.1 $"

import time
from WMCore.Database.DBFormatter import DBFormatter

class GetParentByChildID(DBFormatter):
    sql = """SELECT wb.id, name, block_size, num_files, num_event    
                FROM wq_block wb
                INNER JOIN wq_block_parentage ON wb.id = parent  
                WHERE child = :childID    
          """

    def execute(self, childID, conn = None, transaction = False):
        binds = {"childID": childID}
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)            
        return
