"""
_New_

SQLite implementation of WorkQueueElement.UpdateSubscription
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.UpdateSubscription \
     import UpdateSubscription as UpdateSubscriptionMySQL
     
class UpdateSubscription(UpdateSubscriptionMySQL):
    sql = UpdateSubscriptionMySQL.sql
    