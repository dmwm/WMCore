"""
_UpdatePriority_

Oracle implementation of WorkQueueElement.Priority
"""

__all__ = []
__revision__ = "$Id: UpdatePriority.py,v 1.3 2009/08/27 21:04:30 sryu Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.UpdatePriority \
     import UpdatePriority as UpdatePriorityMySQL

class UpdatePriority(UpdatePriorityMySQL):
    sql = UpdatePriorityMySQL.sql