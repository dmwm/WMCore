"""
_New_

MySQL implementation of WorkQueueElement.UpdateSubscription
"""

__all__ = []
__revision__ = "$Id: UpdateSubscription.py,v 1.2 2009/06/25 18:56:36 sryu Exp $"
__version__ = "$Revision: 1.2 $"

import time
from WMCore.Database.DBFormatter import DBFormatter

class UpdateSubscription(DBFormatter):
    sql = """INSERT INTO wq_element_subs_assoc (element_id, subscription_id) 
                 VALUES (:elementID, :subsID)
          """

    def execute(self, elementID, subscriptionID, conn = None, transaction = False):
        binds = {"elementID":elementID, "subsID":subscriptionID}

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)            
        return
