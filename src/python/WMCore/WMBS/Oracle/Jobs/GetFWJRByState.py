#!/usr/bin/env python
"""
_GetFWJRByState_

Oracle implementation of Jobs.GetFWJRByState
"""

__revision__ = "$Id: GetFWJRByState.py,v 1.1 2009/10/13 20:04:10 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.GetFWJRByState import GetFWJRByState as MySQLGetFWJRByState

class GetFWJRByState(MySQLGetFWJRByState):
    """
    Identical to MySQL version.
    """
    pass
