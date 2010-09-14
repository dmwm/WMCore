#!/usr/bin/env python
"""
_LoadByWMBSID_

Oracle implementation for loading a job by WMBS info
"""


from WMCore.BossAir.MySQL.LoadByWMBSID import LoadByWMBSID as MySQLLoadByWMBSID

class LoadByWMBSID(MySQLLoadByWMBSID):
    """
    _LoadByWMBSID_

    Load all jobs in full by WMBS ID and retry count
    """
