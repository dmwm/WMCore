#!/usr/bin/env python
"""
_ChangeState_

Sqlite implementation of Job.ChangeState
"""

__all__ = []
__revision__ = "$Id: GetJobGroups.py,v 1.2 2009/07/15 21:54:21 meloam Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WMBS.MySQL.Jobs.ChangeState import ChangeState as BaseDAO

class GetJobGroups(BaseDAO):
    pass


