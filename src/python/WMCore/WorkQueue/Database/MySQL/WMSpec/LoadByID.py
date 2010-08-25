"""
_New_

MySQL implementation of Block.LoadByID
"""

__all__ = []
__revision__ = "$Id: LoadByID.py,v 1.1 2009/06/15 20:57:00 sryu Exp $"
__version__ = "$Revision: 1.1 $"

import time
from WMCore.Database.DBFormatter import DBFormatter

class LoadByID(DBFormatter):
    sql = """SELECT name FROM wq_block WHERE id = :specID    
          """

    def execute(self, specID, conn = None, transaction = False):
        binds = {"specID": specID}
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)            
        return
