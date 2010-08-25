"""
_GetExpiredElements_

SQLite implementation of WorkQueueElement.GetExpiredElements
"""

__all__ = []
__revision__ = "$Id: GetExpiredElements.py,v 1.1 2009/11/17 16:53:34 swakef Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.GetExpiredElements \
     import GetExpiredElements as GetExpiredElementsMySQL

class GetExpiredElements(GetExpiredElementsMySQL):
    pass
