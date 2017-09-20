#!/usr/bin/env python
"""
_GetJobGroups_

Oracle implementation of Subscription.GetJobGroups
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WMBS.MySQL.Jobs.ChangeState import ChangeState as BaseDAO

class ChangeState(BaseDAO):
    pass
