"""
_New_

Oracle implementation of Block.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.3 2009/08/27 21:04:30 sryu Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WorkQueue.Database.MySQL.Block.New import New \
     as NewMySQL
     
class New(NewMySQL):
    sql = """INSERT INTO wq_block (name, block_size, num_files, num_events)
                  SELECT :name, :blockSize, :numFiles, :numEvents FROM DUAL
                  WHERE NOT EXISTS
                       (SELECT name FROM wq_block WHERE name = :name)"""
