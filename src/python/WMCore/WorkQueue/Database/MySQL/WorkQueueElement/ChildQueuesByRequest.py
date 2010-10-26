"""
get distinct local workqueue urls assigned from global queue
"""
__all__ = []

from WMCore.Database.DBFormatter import DBFormatter


class ChildQueuesByRequest(DBFormatter):
    """
    get distinct child queue url
    """

    sql = """SELECT distinct(wq.url)
                FROM wq_queues wq
                INNER JOIN wq_element we ON we.child_queue = wq.id
                WHERE request_name = :requestName"""

    def execute(self, requestNames, conn = None, transaction = False):
        binds = []
        for requestName in requestNames:
            binds.append({'requestName' : requestName})

        results = self.dbi.processData(self.sql, binds, conn = conn,
                                       transaction = transaction)

        queues = set()
        results = self.format(results)
        for result in results:
            queues.add(result[0])
        return queues
