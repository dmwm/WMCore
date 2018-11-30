#!/usr/bin/env python
"""
Retrieve basic information about the worker threads such
that it can be pushed to the monitoring tool
"""
from __future__ import division

from WMCore.Agent.Database.MySQL.MonitorWorkers import MonitorWorkers \
    as MySQLMonitorWorkers


class MonitorWorkers(MySQLMonitorWorkers):
    pass
