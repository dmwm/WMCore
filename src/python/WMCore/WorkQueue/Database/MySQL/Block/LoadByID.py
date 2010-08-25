"""
_New_

MySQL implementation of Block.LoadByID
"""

__all__ = []
__revision__ = "$Id: LoadByID.py,v 1.1 2009/06/15 20:56:57 sryu Exp $"
__version__ = "$Revision: 1.1 $"

import time
from WMCore.Database.DBFormatter import DBFormatter

class LoadByID(DBFormatter):
    sql = """SELECT name, block_size, num_files, num_event    
                FROM wq_block 
                WHERE id = :blockID    
          """

    def execute(self, blockID, conn = None, transaction = False):
        binds = {"blockID": blockID}
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)            
        return
