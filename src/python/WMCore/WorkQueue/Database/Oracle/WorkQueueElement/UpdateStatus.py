"""
_UpdateStaus_

Oracle implementation of WorkQueueElement.UpdateStatus
"""

__all__ = []
__revision__ = "$Id: UpdateStatus.py,v 1.3 2009/08/18 23:18:13 swakef Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.UpdateStatus \
     import UpdateStatus as UpdateStatusMySQL

class UpdateStatus(UpdateStatusMySQL):
    sql = UpdateStatusMySQL.sql