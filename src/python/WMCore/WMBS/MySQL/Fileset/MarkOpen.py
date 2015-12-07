#!/usr/bin/env python
"""
_MarkOpen_

MySQL implementation of Fileset.MarkOpen
"""

__all__ = []

import time

from WMCore.Database.DBFormatter import DBFormatter

class MarkOpen(DBFormatter):
    sql = "UPDATE wmbs_fileset SET open = :p_1, last_update = :time WHERE name = :p_2"

    def execute(self, fileset = None, isOpen = True, conn = None,
                transaction = False):
        bindVars = {"p_1": int(isOpen), "p_2": fileset, "time": int(time.time())}
        self.dbi.processData(self.sql, bindVars, conn = conn,
                             transaction = transaction)
        return
