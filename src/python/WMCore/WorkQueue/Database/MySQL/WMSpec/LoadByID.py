"""
_New_

MySQL implementation of Block.LoadByID
"""

__all__ = []



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
