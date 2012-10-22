#!/usr/bin/env python
"""
_SetStatus_

Oracle implementation for altering job status
"""


from WMCore.BossAir.MySQL.SetStatus import SetStatus as MySQLSetStatus

class SetStatus(MySQLSetStatus):
    """
    _SetStatus_

    Set the status of a list of jobs
    """
