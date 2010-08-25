"""
_New_

MySQL implementation of WorkQueueElement.UpdateSubscription
"""

__all__ = []
__revision__ = "$Id: UpdateSubscription.py,v 1.5 2010/05/05 14:36:53 sryu Exp $"
__version__ = "$Revision: 1.5 $"

import time
from WMCore.Database.DBFormatter import DBFormatter

class UpdateSubscription(DBFormatter):
    
# TODO create wq_element_subs_assoc to inforce proper constraint
#    sql = """INSERT INTO wq_element_subs_assoc (element_id, subscription_id) 
#                 VALUES (:elementID, :subsID)
#          """

    sql = """UPDATE wq_element SET subscription_id = :subsID 
                 WHERE id = :elementID
          """
    def execute(self, elementID, subscriptionID, conn = None, transaction = False):
        binds = {"elementID":elementID, "subsID":subscriptionID}

        result = self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)            
        return result[0].rowcount
