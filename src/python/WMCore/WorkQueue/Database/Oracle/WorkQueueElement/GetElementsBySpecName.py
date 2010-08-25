"""
_GetElements_

Oracle implementation of WorkQueueElement.GetElements
"""

__all__ = []
__revision__ = "$Id: GetElementsBySpecName.py,v 1.1 2009/07/02 18:29:24 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.GetElementsBySpecName \
     import GetElementsBySpecName as GetElementsBySpecNameMySQL
     
class GetElementsBySpecName(GetElementsBySpecNameMySQL):
    sql = GetElementsBySpecNameMySQL.sql