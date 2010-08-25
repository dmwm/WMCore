"""
_UpdatePriority_

Oracle implementation of WorkQueueElement.Priority
"""

__all__ = []
__revision__ = "$Id: UpdatePriority.py,v 1.1 2009/08/12 17:01:01 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.UpdatePriority \
     import UpdatePriority as UpdatePriorityMySQL

class UpdateStatus(UpdatePriorityMySQL):
    sql = UpdatePriorityMySQL.sql