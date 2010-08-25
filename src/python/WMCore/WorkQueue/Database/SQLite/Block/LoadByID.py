"""
_New_

SQLite implementation of Block.LoadByID
"""

__all__ = []
__revision__ = "$Id: LoadByID.py,v 1.2 2009/08/18 23:15:09 swakef Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WorkQueue.Database.MySQL.Block.LoadByID import LoadByID \
     as LoadByIDMySQL
     
class LoadByID(LoadByIDMySQL):
    sql = LoadByIDMySQL.sql