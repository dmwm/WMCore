#!/usr/bin/env python
"""
Retrieve basic information about the worker threads such
that it can be pushed to the monitoring tool
"""
from __future__ import division

from WMCore.Database.DBFormatter import DBFormatter


class MonitorWorkers(DBFormatter):
    sql = """SELECT name, last_updated, state, poll_interval, cycle_time
               FROM wm_workers ORDER BY name"""

    def execute(self, conn=None, transaction=False):
        result = self.dbi.processData(self.sql, conn=conn, transaction=transaction)
        return self.formatDict(result)
