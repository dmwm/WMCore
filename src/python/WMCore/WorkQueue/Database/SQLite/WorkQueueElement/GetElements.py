"""
_GetElements_

SQLite implementation of WorkQueueElement.GetElements
"""

__all__ = []
__revision__ = "$Id: GetElements.py,v 1.3 2009/11/12 16:43:32 swakef Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.GetElements \
     import GetElements as GetElementsMySQL

class GetElements(GetElementsMySQL):
    pass
