#!/usr/bin/env python
"""
_GetGroupsByJobState_

SQLite implementation of JobGroup.GetGroupsByJobState
"""

__revision__ = "$Id: GetGroupsByJobState.py,v 1.1 2009/12/17 21:41:20 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.JobGroup.GetGroupsByJobState import GetGroupsByJobState as MySQLGetGroupsByJobState

class GetGroupsByJobState(MySQLGetGroupsByJobState):
    pass
