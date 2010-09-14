#!/usr/bin/env python
"""
_LoadRunning_

Oracle implementation for loading a job by scheduler status
"""


from WMCore.BossAir.MySQL.LoadRunning import LoadRunning as MySQLLoadRunning

class LoadRunning(MySQLLoadRunning):
    """
    _LoadRunning_

    Load all jobs with a certain scheduler status
    """

