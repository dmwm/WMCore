"""
_GetExpiredElements_

SQLite implementation of WorkQueueElement.GetExpiredElements
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.GetExpiredElements \
     import GetExpiredElements as GetExpiredElementsMySQL

class GetExpiredElements(GetExpiredElementsMySQL):
    pass
