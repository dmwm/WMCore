"""
_New_

MySQL implementation of WorkQueueElement.UpdateSubscription
"""

__all__ = []
__revision__ = "$Id: UpdateSubscription.py,v 1.2 2009/08/18 23:18:13 swakef Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.UpdateSubscription \
     import UpdateSubscription as UpdateSubscriptionMySQL
     
class UpdateSubscription(UpdateSubscriptionMySQL):
    sql = UpdateSubscriptionMySQL.sql
    