"""
_GetHeartbeatInfo_

MySQL implementation of GetHeartbeatInfo
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class GetAllHeartbeatInfo(DBFormatter):

    sql = """SELECT comp.name as name, comp.pid, worker.name as worker_name,
                 worker.state, worker.last_updated, comp.update_threshold,
                 worker.poll_interval, worker.cycle_time, worker.outcome,
                 worker.last_error, worker.error_message
             FROM wm_workers worker
             INNER JOIN wm_components comp ON comp.id = worker.component_id
             """

    def execute(self, conn = None, transaction = False):

        result = self.dbi.processData(self.sql, conn = conn,
                             transaction = transaction)
        return self.formatDict(result)
