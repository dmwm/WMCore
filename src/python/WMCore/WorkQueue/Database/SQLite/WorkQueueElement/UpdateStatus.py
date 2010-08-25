"""
_UpdateStaus_

SQLite implementation of WorkQueueElement.UpdateStatus
"""

__all__ = []
__revision__ = "$Id: UpdateStatus.py,v 1.4 2009/11/12 16:43:32 swakef Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.UpdateStatus \
     import UpdateStatus as UpdateStatusMySQL

class UpdateStatus(UpdateStatusMySQL):
    queue_insert_sql = UpdateStatusMySQL.queue_insert_sql.replace('IGNORE', 'OR IGNORE')
