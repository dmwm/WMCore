"""
_AddParent_

Oracle implementation of Block.AddParent
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.Data.AddParent import AddParent \
     as AddParentMySQL

class AddParent(AddParentMySQL):
    sql = AddParentMySQL.sql
