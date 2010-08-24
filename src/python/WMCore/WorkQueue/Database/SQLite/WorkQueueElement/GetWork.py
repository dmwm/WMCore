"""

SQLite implementation of WorkQueueElement.GetWork
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.GetWork \
     import GetWork as GetWorkMySQL

class GetWork(GetWorkMySQL):
    sql = GetWorkMySQL.sql
