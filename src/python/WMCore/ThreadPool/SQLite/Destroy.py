#!/usr/bin/env python
"""
_ChangeState_

Sqlite implementation of Job.ChangeState
"""

__all__ = []
__revision__ = "$Id: Destroy.py,v 1.1 2009/07/15 21:46:27 meloam Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.ThreadPool.Mysql.Destroy import Destroy as BaseDAO

class Destroy(BaseDAO):
    pass


