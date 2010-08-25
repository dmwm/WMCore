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
    sql = """SELECT name, url FROM wq_wmspec WHERE id = :specID    
          """

    def execute(self, specID, conn = None, transaction = False):
        binds = {"specID": specID}
        results = self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)            
        return self.formatDict(results)
