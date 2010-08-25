"""
_GetElements_

SQLite implementation of WorkQueueElement.GetElements
"""

__all__ = []
__revision__ = "$Id: GetElements.py,v 1.2 2009/08/18 23:18:12 swakef Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.GetElements \
     import GetElements as GetElementsMySQL
     
class GetElements(GetElementsMySQL):
    sql = GetElementsMySQL.sql