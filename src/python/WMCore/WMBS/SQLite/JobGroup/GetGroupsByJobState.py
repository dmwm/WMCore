#!/usr/bin/env python
"""
_GetGroupsByJobState_

SQLite implementation of JobGroup.GetGroupsByJobState
"""




from WMCore.WMBS.MySQL.JobGroup.GetGroupsByJobState import GetGroupsByJobState as MySQLGetGroupsByJobState

class GetGroupsByJobState(MySQLGetGroupsByJobState):
    pass
