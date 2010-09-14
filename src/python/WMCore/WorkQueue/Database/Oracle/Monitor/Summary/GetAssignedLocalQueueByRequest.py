"""
_GetAssignedLocalQueueByRequest_

Oracle implementation of Monitor.Summary.GetAssignedLocalQueueByRequest
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.Monitor.Summary.GetAssignedLocalQueueByRequest \
     import GetAssignedLocalQueueByRequest as GetAssignedLocalQueueByRequestMySQL

class GetAssignedLocalQueueByRequest(GetAssignedLocalQueueByRequestMySQL):
    pass