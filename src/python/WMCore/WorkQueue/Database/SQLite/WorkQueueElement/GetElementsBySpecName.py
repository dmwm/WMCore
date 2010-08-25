"""
_GetElements_

SQLite implementation of WorkQueueElement.GetElements
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.GetElementsBySpecName \
     import GetElementsBySpecName as GetElementsBySpecNameMySQL
     
class GetElementsBySpecName(GetElementsBySpecNameMySQL):
    sql = GetElementsBySpecNameMySQL.sql