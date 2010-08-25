#!/usr/bin/env python
"""
_ChangeState_

Sqlite implementation of Job.ChangeState
"""

__all__ = []
__revision__ = "$Id: GetJobGroups.py,v 1.3 2009/07/15 21:55:17 meloam Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WMBS.MySQL.Subscriptions.GetJobGroups import GetJobGroups as BaseDAO

class GetJobGroups(BaseDAO):
    pass


