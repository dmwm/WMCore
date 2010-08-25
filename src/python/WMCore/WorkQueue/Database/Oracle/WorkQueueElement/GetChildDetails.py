"""
_GetChildDetails_

Oracle implementation of WorkQueueElement.GetChildDetails
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.GetChildDetails \
     import GetChildDetails as GetChildDetailsMySQL

class GetChildDetails(GetChildDetailsMySQL):
    """
    The same as MySql implementation
    """
