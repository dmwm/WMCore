"""
get distinct local workqueue urls assigned from global queue
"""
__all__ = []

from WMCore.Database.DBFormatter import DBFormatter


class GetAssignedLocalQueueByRequest(DBFormatter):
    """
    Based on assumption that request name is unique per wmspec

    """

    sql = """SELECT we.request_name, wq.url AS local_queue
                FROM wq_element we
                LEFT OUTER JOIN wq_queues wq ON wq.id = we.child_queue
                GROUP BY we.request_name, wq.url
                ORDER BY we.id DESC"""

    def execute(self, conn = None, transaction = False):
        results = self.dbi.processData(self.sql, conn = conn,
                                       transaction = transaction)

        return self.formatDict(results)