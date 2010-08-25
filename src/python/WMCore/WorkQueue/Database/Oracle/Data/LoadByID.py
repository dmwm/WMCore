"""
_New_

Oracle implementation of Block.LoadByID
"""

__all__ = []
__revision__ = "$Id: LoadByID.py,v 1.1 2009/09/03 15:44:17 swakef Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.Data.LoadByID import LoadByID \
     as LoadByIDMySQL

class LoadByID(LoadByIDMySQL):
    sql = LoadByIDMySQL.sql
