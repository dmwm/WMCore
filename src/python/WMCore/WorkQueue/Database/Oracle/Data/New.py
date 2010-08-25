"""
_New_

Oracle implementation of Block.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.1 2009/09/03 15:44:17 swakef Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.Data.New import New \
     as NewMySQL

class New(NewMySQL):
    sql = """INSERT INTO wq_data (name)
                  SELECT :name FROM DUAL
                  WHERE NOT EXISTS
                       (SELECT name FROM wq_data WHERE name = :name)"""
