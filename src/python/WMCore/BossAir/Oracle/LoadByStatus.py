#!/usr/bin/env python
"""
_LoadByStatus_

Oracle implementation for loading a job by scheduler status
"""


from WMCore.BossAir.MySQL.LoadByStatus import LoadByStatus as MySQLLoadByStatus

class LoadByStatus(MySQLLoadByStatus):
    """
    _LoadByStatus_

    Load all jobs with a certain scheduler status
    """
