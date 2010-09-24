"""
Get elements that have updates for ReqMgr

MySQL implementation
"""

__all__ = []

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.GetElements import GetElements


class GetReqMgrUpdateNeeded(GetElements):
    # to get the correct aggregate request status we need all elements
    # from a request if one of its elements has changed
    sql = GetElements.sql + """ WHERE we.request_name IN (SELECT DISTINCT request_name FROM wq_element
                                                      WHERE request_name IS NOT NULL AND reqmgr_time <= update_time)
                        """

    def execute(self, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, conn = conn,
                                      transaction = transaction)
        return GetElements.formatWQE(self, self.formatDict(result))