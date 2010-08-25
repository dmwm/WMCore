"""
_UpdatePriority_

Oracle implementation of WorkQueueElement.Priority
"""

__all__ = []
__revision__ = "$Id: UpdatePriority.py,v 1.4 2010/02/08 19:05:46 sryu Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.UpdatePriority \
     import UpdatePriority as UpdatePriorityMySQL

class UpdatePriority(UpdatePriorityMySQL):
    sql = UpdatePriorityMySQL.sql
    
    def execute(self, priority, workflows, conn = None, transaction = False):
        UpdatePriorityMySQL.execute(self, priority, workflows, conn, transaction)
        return True