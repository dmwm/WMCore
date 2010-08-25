"""
_New_

SQLite implementation of Block.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.2 2009/08/18 23:18:13 swakef Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WorkQueue.Database.MySQL.Block.New import New \
     as NewMySQL

class New(NewMySQL):
    sql = """INSERT OR IGNORE INTO wq_block (name, block_size, num_files, num_events)
                 VALUES (:name, :blockSize, :numFiles, :numEvents)
          """
