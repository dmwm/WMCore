"""
_GetElements_

Oracle implementation of WorkQueueElement.GetElements
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.GetElements \
     import GetElements as GetElementsMySQL
     
class GetElements(GetElementsMySQL):
    sql = GetElementsMySQL.sql