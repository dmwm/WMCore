"""

SQLite implementation of WorkQueueElement.GetReqMgrUpdateNeeded
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.GetReqMgrUpdateNeeded \
     import GetReqMgrUpdateNeeded as GetReqMgrUpdateNeededMySQL

class GetReqMgrUpdateNeeded(GetReqMgrUpdateNeededMySQL):
    pass
