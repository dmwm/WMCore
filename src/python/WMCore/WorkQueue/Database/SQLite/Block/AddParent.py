"""
_New_

SQLite implementation of Block.AddParent
"""

__all__ = []
__revision__ = "$Id: AddParent.py,v 1.2 2009/08/18 23:18:13 swakef Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WorkQueue.Database.MySQL.Block.AddParent import AddParent \
     as AddParentMySQL

class AddParent(AddParentMySQL):
    sql = AddParentMySQL.sql