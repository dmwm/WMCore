"""
_GetChildDetails_

SQLite implementation of WorkQueueElement.GetChildDetails
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.GetChildDetails \
     import GetChildDetails as GetChildDetailsMySQL

class GetChildDetails(GetChildDetailsMySQL):
        pass
