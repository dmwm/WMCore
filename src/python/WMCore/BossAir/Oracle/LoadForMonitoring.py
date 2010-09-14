#!/usr/bin/env python
"""
_LoadForMonitoring_

Oracle implementation for loading a job by scheduler status
"""


from WMCore.BossAir.MySQL.LoadForMonitoring import LoadForMonitoring as MySQLLoadForMonitoring

class LoadForMonitoring(MySQLLoadForMonitoring):
    """
    _LoadForMonitoring_

    Load all jobs with a certain scheduler status including
    all the joined information.
    """
