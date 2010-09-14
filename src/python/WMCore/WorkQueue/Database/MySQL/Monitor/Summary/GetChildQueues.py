"""
get distinct local workqueue urls assigned from global queue
"""
__all__ = []

from WMCore.Database.DBFormatter import DBFormatter


class GetChildQueues(DBFormatter):
    """
    get disticnt child queue url
    """

    sql = """SELECT url AS local_queue
                FROM wq_queues"""

    def execute(self, conn = None, transaction = False):
        results = self.dbi.processData(self.sql, conn = conn,
                                       transaction = transaction)

        queues = []
        results = self.format(results)
        for result in results:
            queues.append(result[0])
        return queues
