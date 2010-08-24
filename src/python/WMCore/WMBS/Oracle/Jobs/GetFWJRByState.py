#!/usr/bin/env python
"""
_GetFWJRByState_

Oracle implementation of Jobs.GetFWJRByState
"""




from WMCore.WMBS.MySQL.Jobs.GetFWJRByState import GetFWJRByState as MySQLGetFWJRByState

class GetFWJRByState(MySQLGetFWJRByState):
    """
    Identical to MySQL version.
    """
    pass
