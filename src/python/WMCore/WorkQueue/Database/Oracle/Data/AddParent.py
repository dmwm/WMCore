"""
_New_

Oracle implementation of Block.AddParent
"""

__all__ = []
__revision__ = "$Id: AddParent.py,v 1.1 2009/09/03 15:44:17 swakef Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.Data.AddParent import AddParent \
     as AddParentMySQL

class AddParent(AddParentMySQL):
    sql = AddParentMySQL.sql
