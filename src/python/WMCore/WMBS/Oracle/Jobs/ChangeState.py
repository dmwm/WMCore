#!/usr/bin/env python
"""
_GetJobGroups_

Sqlite implementation of Subscription.GetJobGroups
"""

__all__ = []
__revision__ = "$Id: ChangeState.py,v 1.1 2009/07/17 20:21:17 meloam Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WMBS.MySQL.Jobs.ChangeState import ChangeState as BaseDAO

class ChangeState(BaseDAO):
    pass


