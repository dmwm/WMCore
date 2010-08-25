"""

SQLite implementation of WorkQueueElement.GetParentId
"""

__all__ = []
__revision__ = "$Id: GetParentId.py,v 1.1 2009/09/17 15:37:53 swakef Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.GetParentId \
     import GetParentId as GetParentIdMySQL

class GetParentId(GetParentIdMySQL):
    sql = GetParentIdMySQL.sql
