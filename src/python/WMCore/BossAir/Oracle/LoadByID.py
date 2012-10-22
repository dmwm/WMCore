#!/usr/bin/env python
"""
_LoadByID_

Oracle implementation for loading a job by scheduler status
"""


from WMCore.BossAir.MySQL.LoadByID import LoadByID as MySQLLoadByID

class LoadByID(MySQLLoadByID):
    """
    _LoadByID_

    Load all jobs in full by ID
    """
