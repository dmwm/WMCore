"""
_GetElements_

SQLite implementation of WorkQueueElement.GetElements
"""

__all__ = []
__revision__ = "$Id: GetElements.py,v 1.1 2009/07/17 14:25:28 swakef Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.GetElements \
     import GetElements as GetElementsMySQL
     
class GetElements(GetElementsMySQL):
    sql = GetElementsMySQL.sql