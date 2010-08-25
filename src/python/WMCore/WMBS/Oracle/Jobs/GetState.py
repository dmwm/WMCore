#!/usr/bin/env python
"""
_GetState_

Oracle implementation of Jobs.GetState
"""

__all__ = []
__revision__ = "$Id: GetState.py,v 1.1 2009/07/10 20:02:49 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.GetState import GetState as MySQLGetState

class GetState(MySQLGetState):
    """
    Right now this is the same as the MySQL version.

    """
