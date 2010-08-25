#!/usr/bin/env python
"""
_ChangeState_

Sqlite implementation of Destroy for ThreadPool
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter
from WMCore.ThreadPool.MySQL.Destroy import Destroy as BaseDAO

class Destroy(BaseDAO):

    def __init__(self):

        BaseDAO.__init__(self)

        self.delete["04tp_queued_process"] = "DROP TABLE tp_queued_process_enum"


