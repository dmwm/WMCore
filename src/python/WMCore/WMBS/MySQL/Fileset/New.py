#!/usr/bin/env python
"""
_New_

MySQL implementation of Fileset.New
"""

__all__ = []



import time

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = """INSERT INTO wmbs_fileset (name, last_update, open)
               VALUES (:NAME, :LAST_UPDATE, :OPEN)"""

    def getBinds(self, name = None, open = False):
        bindVars = {}
        bindVars["NAME"] = name
        bindVars["OPEN"] = int(open)
        bindVars["LAST_UPDATE"] = int(time.time())
        return bindVars

    def execute(self, name = None, open = False, conn = None,
                transaction = False):
        self.dbi.processData(self.sql, self.getBinds(name, open),
                         conn = conn, transaction = transaction)
        return
