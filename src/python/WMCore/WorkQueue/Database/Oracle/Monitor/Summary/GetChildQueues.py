"""
_GetChildQueues_

Oracle implementation of Monitor.Summary.GetChildQueues
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.Monitor.Summary.GetChildQueues \
     import GetChildQueues as GetChildQueuesMySQL

class GetChildQueues(GetChildQueuesMySQL):
    pass