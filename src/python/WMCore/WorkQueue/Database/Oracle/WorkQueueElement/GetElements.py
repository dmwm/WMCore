"""
_GetElements_

Oracle implementation of WorkQueueElement.GetElements
"""

__all__ = []
__revision__ = "$Id: GetElements.py,v 1.3 2009/08/18 23:18:13 swakef Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.GetElements \
     import GetElements as GetElementsMySQL
     
class GetElements(GetElementsMySQL):
    sql = GetElementsMySQL.sql