#!/usr/bin/env python
"""
_GetCountByState_

Oracle implementation of Jobs.GetCountByState
"""

from WMCore.WMBS.MySQL.Jobs.GetCountByState import GetCountByState as MySQLGetCountByState


class GetCountByState(MySQLGetCountByState):
    """
    Identical to MySQL version.
    """
    pass
