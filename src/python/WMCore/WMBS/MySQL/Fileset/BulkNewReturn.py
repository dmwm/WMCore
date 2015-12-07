#!/usr/bin/env python
"""
_BulkNewReturn_

MySQL implementation of Fileset.BulkNewReturn
"""

__all__ = []



import time

from WMCore.Database.DBFormatter import DBFormatter

class BulkNewReturn(DBFormatter):
    """
    Creates filesets in bulk and returns their IDs

    """
    sql = """INSERT INTO wmbs_fileset (name, last_update, open)
               VALUES (:NAME, :LAST_UPDATE, :OPEN)"""

    returnSQL = """SELECT ID FROM wmbs_fileset
                   WHERE name = :NAME
                   AND last_update = :LAST_UPDATE
                   AND open = :OPEN"""

    def getBinds(self, name = None, open = False):
        bindVars = {}
        bindVars["NAME"] = name
        bindVars["OPEN"] = int(open)
        bindVars["LAST_UPDATE"] = int(time.time())
        return bindVars

    def execute(self, nameList = None, open = False, conn = None,
                transaction = False):
        """
        This can take a bulk argument list that should only be the names

        """
        binds = []
        for name in nameList:
            binds.append(self.getBinds(name = name, open = open))

        self.dbi.processData(self.sql, binds,
                             conn = conn, transaction = transaction)

        result = self.dbi.processData(self.returnSQL, binds,
                                      conn = conn, transaction = transaction)

        res = self.format(result)

        # This should be a list
        final = []
        for entry in res:
            final.append(entry[0])

        return final
