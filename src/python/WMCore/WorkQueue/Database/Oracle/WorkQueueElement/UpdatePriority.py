"""
_UpdatePriority_

Oracle implementation of WorkQueueElement.Priority
"""

__all__ = []
__revision__ = "$Id: UpdatePriority.py,v 1.5 2010/05/05 14:36:25 sryu Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.UpdatePriority \
     import UpdatePriority as UpdatePriorityMySQL

class UpdatePriority(UpdatePriorityMySQL):
    sql = UpdatePriorityMySQL.sql