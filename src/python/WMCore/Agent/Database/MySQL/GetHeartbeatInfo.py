"""
_GetHeartbeatInfo_

Fetches the hearbeat info for all worker threads associated to
the component name passed in.
"""

__all__ = []

from WMCore.Database.DBFormatter import DBFormatter


class GetHeartbeatInfo(DBFormatter):
    sql = """SELECT comp.name as name, comp.pid, worker.name as worker_name,
                    worker.state, worker.last_updated, comp.update_threshold,
                    worker.poll_interval, worker.cycle_time, worker.outcome,
                    worker.last_error, worker.error_message
             FROM wm_workers worker
             INNER JOIN wm_components comp ON comp.id = worker.component_id
             WHERE comp.name = :component_name
             ORDER BY worker.last_updated ASC
             """

    def execute(self, compName, conn=None, transaction=False):
        bind = {"component_name": compName}

        result = self.dbi.processData(self.sql, bind, conn=conn,
                                      transaction=transaction)
        return self.formatDict(result)
