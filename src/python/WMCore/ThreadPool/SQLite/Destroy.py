#!/usr/bin/env python
"""
_ChangeState_

Sqlite implementation of Destroy for ThreadPool
"""

__all__ = []
__revision__ = "$Id: Destroy.py,v 1.4 2009/08/12 17:18:05 meloam Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.ThreadPool.MySQL.Destroy import Destroy as BaseDAO

class Destroy(BaseDAO):

    def __init__(self):

        BaseDAO.__init__(self)

        self.delete["04tp_queued_process"] = "DROP TABLE tp_queued_process_enum"


