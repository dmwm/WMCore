"""
_New_

Oracle implementation of Block.AddParent
"""

__all__ = []
__revision__ = "$Id: AddParent.py,v 1.1 2009/06/25 18:55:51 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.Block.AddParent import AddParent \
     as AddParentMySQL

class AddParent(AddParentMySQL):
    sql = AddParentMySQL.sql