"""
_UpdateStaus_

MySQL implementation of WorkQueueElement.UpdateStatus
"""

__all__ = []
__revision__ = "$Id: UpdateStatus.py,v 1.7 2010/04/23 18:51:47 sryu Exp $"
__version__ = "$Revision: 1.7 $"


from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WorkQueue.Database import States
import time

class UpdateStatus(DBFormatter):
    sql1 = """UPDATE wq_element SET status = :status, update_time = :now"""
    sql2 = """, child_queue = (SELECT id FROM wq_queues WHERE url = :queue)"""
    sql3 = """ WHERE %s = :id"""

    queue_insert_sql = """INSERT IGNORE INTO wq_queues (url) VALUES (:queue)"""

    def execute(self, status, ids, id_type = 'id',
                child_queue = None,
                conn = None, transaction = False):
        if status not in States:
            raise RuntimeError, "Invalid state: %s" % status
        if id_type not in ('parent_queue_id', 'id', 'subscription_id'):
            raise RuntimeError, "Invalid id_type: %s" % id_type

        now = int(time.time())
        binds = [{"status": States[status],
                  "now" : now,
                  'id' : x} for x in ids]

        sql = self.sql1
        if child_queue:
            self.dbi.processData(self.queue_insert_sql,
                                 {'queue' : child_queue},
                                 conn = conn,
                                 transaction = transaction)
            [ x.__setitem__('queue', child_queue) for x in binds]
            sql += self.sql2
        sql += self.sql3 % id_type

        result = self.dbi.processData(sql, binds, conn = conn,
                                      transaction = transaction)
        return result[0].rowcount
