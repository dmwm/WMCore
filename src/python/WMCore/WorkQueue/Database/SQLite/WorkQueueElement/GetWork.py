"""

SQLite implementation of WorkQueueElement.GetWork
"""

__all__ = []
__revision__ = "$Id: GetWork.py,v 1.2 2009/08/27 21:03:17 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.GetWork \
     import GetWork as GetWorkMySQL

class GetWork(GetWorkMySQL):
    sql = GetWorkMySQL.sql
