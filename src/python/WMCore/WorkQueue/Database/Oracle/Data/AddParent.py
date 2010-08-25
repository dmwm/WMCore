"""
_AddParent_

Oracle implementation of Block.AddParent
"""

__all__ = []
__revision__ = "$Id: AddParent.py,v 1.2 2010/08/06 21:05:02 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WorkQueue.Database.MySQL.Data.AddParent import AddParent \
     as AddParentMySQL

class AddParent(AddParentMySQL):
    sql = AddParentMySQL.sql
