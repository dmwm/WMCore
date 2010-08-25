"""
_UpdatePriority_

MySQL implementation of WorkQueueElement.UpdateReqMgr
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class UpdateReqMgr(DBFormatter):
    sql = """UPDATE wq_element SET reqmgr_time = :when
             WHERE id = :id"""

    def execute(self, when, ids, conn = None, transaction = False):
        binds = [{"when": when, "id" : x} for x in ids]
        result = self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return result[0].rowcount