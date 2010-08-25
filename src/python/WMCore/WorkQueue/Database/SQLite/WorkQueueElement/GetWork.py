"""

SQLite implementation of WorkQueueElement.GetWork
"""

__all__ = []
__revision__ = "$Id: GetWork.py,v 1.1 2009/08/18 23:18:12 swakef Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.GetWork \
     import GetWork as GetWorkMySQL

class GetWork(GetWorkMySQL):
    sql = GetWorkMySQL.sql.replace("NOW()", """strftime('%s', 'now')""")
