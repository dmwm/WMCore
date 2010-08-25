"""

Oracle implementation of WorkQueueElement.GetWork
"""

__all__ = []
__revision__ = "$Id: GetWork.py,v 1.1 2009/08/27 21:04:30 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.GetWork \
     import GetWork as GetWorkMySQL

class GetWork(GetWorkMySQL):
    sql = GetWorkMySQL.sql
