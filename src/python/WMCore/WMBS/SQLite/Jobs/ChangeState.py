#!/usr/bin/env python
"""
_GetJobGroups_

Sqlite implementation of Subscription.GetJobGroups
"""

__all__ = []
__revision__ = "$Id: ChangeState.py,v 1.1 2009/07/15 21:44:36 meloam Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WMBS.MySQL.Subscriptions.GetJobGroups import GetJobGroups as BaseDAO

class GetJobGroups(BaseDAO):
    pass


