"""
_New_

MySQL implementation of Block.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.2 2009/06/24 21:00:24 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = """INSERT INTO wq_block (name, block_size, num_files, num_events) 
                 VALUES (:name, :blockSize, :numFiles, :numEvents)
          """

    def execute(self, name, blockSize, numFiles, numEvents,
                conn = None, transaction = False):
        binds = {"name": name, "blockSize": blockSize, "numFiles": numFiles,
                 "numEvents": numEvents}

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)            
        return
