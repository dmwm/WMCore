"""

SQLite implementation of WorkQueueElement.GetParentId
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.GetParentId \
     import GetParentId as GetParentIdMySQL

class GetParentId(GetParentIdMySQL):
    sql = GetParentIdMySQL.sql
