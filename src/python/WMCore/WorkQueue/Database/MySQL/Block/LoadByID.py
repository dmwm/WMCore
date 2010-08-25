"""
_New_

MySQL implementation of Block.LoadByID
"""

__all__ = []
__revision__ = "$Id: LoadByID.py,v 1.2 2009/06/24 21:00:24 sryu Exp $"
__version__ = "$Revision: 1.2 $"

import time
from WMCore.Database.DBFormatter import DBFormatter

class LoadByID(DBFormatter):
    sql = """SELECT name, block_size, num_files, num_events    
                FROM wq_block 
                WHERE id = :blockID    
          """

    def execute(self, blockID, conn = None, transaction = False):
        binds = {"blockID": blockID}
        results = self.dbi.processData(self.sql, binds, conn = conn,
                                       transaction = transaction) 
        return self.formatDict(results)
