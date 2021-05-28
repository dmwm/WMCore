#!/usr/bin/env python
"""
_SetStatus_

MySQL implementation of DBSBufferFiles.SetStatus
"""




from WMCore.Database.DBFormatter import DBFormatter

class SetStatus(DBFormatter):
    sql = "UPDATE dbsbuffer_file SET status = :status WHERE lfn = :lfn"

    def execute(self, lfns, status, conn = None, transaction = None):
        if not isinstance(lfns, list):
            lfns = [lfns]

        bindVars = []
        for lfn in lfns:
            bindVars.append({"lfn": lfn, "status": status})

        result = self.dbi.processData(self.sql, bindVars, conn = conn,
                                      transaction = transaction)
        return
