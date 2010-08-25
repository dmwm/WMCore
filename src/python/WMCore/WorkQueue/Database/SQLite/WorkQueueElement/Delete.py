"""
_UpdateStaus_

SQLite implementation of WorkQueueElement.Delete
"""

__all__ = []
__revision__ = "$Id: Delete.py,v 1.1 2010/06/02 14:42:10 swakef Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.Delete import Delete \
     as DeleteMySQL

class Delete(DeleteMySQL):
    sql = DeleteMySQL.sql