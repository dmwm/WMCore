"""
_UpdateProgress_

MySQL implementation of WorkQueueElement.UpdateProgress
"""

__all__ = []




from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WorkQueue.Database import States
import time

class UpdateProgress(DBFormatter):
    sql1 = """UPDATE wq_element SET update_time = :now"""
    sql2 = """, %s = :%s"""
    sql3 = """ WHERE %s = :id"""

    mapping = {'FilesProcessed' : 'files_processed',
               'PercentComplete' : 'percent_complete',
               'PercentSuccess' : 'percent_success'}

    def execute(self, ids, values, id_type = 'id',
                conn = None, transaction = False):
        if len(ids) == 0:
            # if ids are not passed just declare success
            return 1

        if id_type not in ('parent_queue_id', 'id', 'subscription_id'):
            raise RuntimeError, "Invalid id_type: %s" % id_type

        sql = self.sql1
        newvalues = {}
        for key, value in self.mapping.items():
            if key in values:
                sql += self.sql2 % (value, value)
                newvalues[value] = values[key]
        sql += self.sql3 % id_type

        now = int(time.time())
        binds = [{"now" : now,
                  'id' : x} for x in ids]
        [bind.update(newvalues) for bind in binds]

        result = self.dbi.processData(sql, binds, conn = conn,
                                      transaction = transaction)
        return result[0].rowcount