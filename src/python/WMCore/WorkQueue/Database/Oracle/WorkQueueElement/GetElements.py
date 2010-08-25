"""
_GetElements_

Oracle implementation of WorkQueueElement.GetElements
"""

__all__ = []
__revision__ = "$Id: GetElements.py,v 1.2 2009/07/02 18:29:24 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.GetElements \
     import GetElements as GetElementsMySQL
     
class GetElements(GetElementsMySQL):
    sql = GetElementsMySQL.sql