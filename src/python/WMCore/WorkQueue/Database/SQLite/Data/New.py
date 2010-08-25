"""
_New_

SQLite implementation of Block.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.1 2009/09/03 15:44:19 swakef Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.Data.New import New \
     as NewMySQL

class New(NewMySQL):
    sql = NewMySQL.sql.replace('IGNORE', 'OR IGNORE')
