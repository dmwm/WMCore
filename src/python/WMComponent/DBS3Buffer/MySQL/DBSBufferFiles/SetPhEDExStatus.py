#!/usr/bin/env python
"""
_SetPhEDExStatus_

MySQL implementation of DBSBufferFiles.SetPhEDExStatus
"""

from WMCore.Database.DBFormatter import DBFormatter

class SetPhEDExStatus(DBFormatter):
    sql = "UPDATE dbsbuffer_file SET in_phedex = :status WHERE lfn = :lfn"

    def execute(self, lfns, status, conn = None, transaction = False):

        if not isinstance(lfns, list):
            lfns = [lfns]

        if len(lfns) < 1:
            return

        bindVars = []
        for lfn in lfns:
            bindVars.append({"lfn": lfn, "status": status})

        self.dbi.processData(self.sql, bindVars, conn = conn,
                             transaction = transaction)

        return
