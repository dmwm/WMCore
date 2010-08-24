"""
_New_

SQLite implementation of Block.GetParentByChildID
"""

__all__ = []



import time
from WMCore.WorkQueue.Database.MySQL.Data.GetParentsByChildID \
    import GetParentsByChildID as GetParentsByChildIDMySQL

class GetParentsByChildID(GetParentsByChildIDMySQL):
    sql = GetParentsByChildIDMySQL.sql
