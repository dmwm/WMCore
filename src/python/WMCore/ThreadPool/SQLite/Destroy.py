#!/usr/bin/env python
"""
_ChangeState_

Sqlite implementation of Destroy for ThreadPool
"""

__all__ = []
__revision__ = "$Id: Destroy.py,v 1.3 2009/07/20 17:39:11 mnorman Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.ThreadPool.MySQL.Destroy import Destroy as BaseDAO

class Destroy(BaseDAO):

    def __init__(self):

        BaseDAO.__init__(self)

        self.create["04tp_queued_process"] = "DROP TABLE tp_queued_process_enum"


