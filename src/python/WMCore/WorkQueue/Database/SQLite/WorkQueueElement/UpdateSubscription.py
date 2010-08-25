"""
_New_

SQLite implementation of WorkQueueElement.UpdateSubscription
"""

__all__ = []
__revision__ = "$Id: UpdateSubscription.py,v 1.1 2009/07/17 14:25:28 swakef Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.UpdateSubscription \
     import UpdateSubscription as UpdateSubscriptionMySQL
     
class UpdateSubscription(UpdateSubscriptionMySQL):
    sql = UpdateSubscriptionMySQL.sql
    