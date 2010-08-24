#!/usr/bin/env python
"""
_GetState_

Oracle implementation of Jobs.GetState
"""

__all__ = []



from WMCore.WMBS.MySQL.Jobs.GetState import GetState as MySQLGetState

class GetState(MySQLGetState):
    """
    Right now this is the same as the MySQL version.

    """
