"""

Oracle implementation of WorkQueueElement.GetRequestNamesByIDs
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.GetRequestNamesByIDs \
     import GetRequestNamesByIDs as GetRequestNamesByIDsMySQL

class GetRequestNamesByIDs(GetRequestNamesByIDsMySQL):
    pass