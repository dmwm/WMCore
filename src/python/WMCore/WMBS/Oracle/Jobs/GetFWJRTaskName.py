#!/usr/bin/env python
"""
_GetFWJRTaskName_

Oracle implementation of Jobs.GetFWJRTaskName
"""

from WMCore.WMBS.MySQL.Jobs.GetFWJRTaskName import GetFWJRTaskName as MySQLGetFWJRTaskName

class GetFWJRTaskName(MySQLGetFWJRTaskName):
    """
    Identical to MySQL version.
    """
    pass
