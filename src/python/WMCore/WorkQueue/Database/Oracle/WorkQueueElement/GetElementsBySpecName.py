"""
_GetElements_

Oracle implementation of WorkQueueElement.GetElements
"""

__all__ = []
__revision__ = "$Id: GetElementsBySpecName.py,v 1.2 2009/08/18 23:18:13 swakef Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.GetElementsBySpecName \
     import GetElementsBySpecName as GetElementsBySpecNameMySQL
     
class GetElementsBySpecName(GetElementsBySpecNameMySQL):
    sql = GetElementsBySpecNameMySQL.sql