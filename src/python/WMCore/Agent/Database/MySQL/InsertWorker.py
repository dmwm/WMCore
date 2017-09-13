"""
_InsertComponent_

MySQL implementation of InsertWorker
"""

__all__ = []



import time
from WMCore.Database.DBFormatter import DBFormatter

class InsertWorker(DBFormatter):

    sql = """INSERT INTO wm_workers (component_id, name, last_updated, state, pid, poll_interval, cycle_time)
             VALUES ((SELECT id FROM wm_components WHERE name = :component_name),
                     :worker_name, :last_updated, :state, :pid, :poll_interval, :cycle_time)
             """

    def execute(self, componentName, workerName, state, pid, pollInt, cycleTime,
                conn = None, transaction = False):

        binds = {"component_name": componentName,
                 "worker_name": workerName,
                 "last_updated": int(time.time()),
                 "state": state,
                 "pid": pid,
                 "poll_interval": pollInt,
                 "cycle_time": cycleTime}

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
