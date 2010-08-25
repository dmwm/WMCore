"""
_New_

SQLite implementation of Block.LoadByID
"""

__all__ = []
__revision__ = "$Id: LoadByID.py,v 1.1 2009/07/17 14:25:29 swakef Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.Block.LoadByID import LoadByID \
     as LoadByIDMySQL
     
class LoadByID(LoadByIDMySQL):
    sql = LoadByIDMySQL.sql