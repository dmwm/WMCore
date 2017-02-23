#!/usr/bin/env python
"""
_UpdateFiles_

MySQL implementation of DBS3Buffer.UpdateFiles
"""

from WMCore.Database.DBFormatter import DBFormatter

class UpdateFiles(DBFormatter):
    sql = """UPDATE dbsbuffer_file SET status = :status
             WHERE block_id = (SELECT id FROM dbsbuffer_block WHERE blockname = :block)"""

    def execute(self, blocks, status, conn = None, transaction = False):
        bindVars = []
        for block in blocks:
            bindVars.append({"block": block.getName(), "status": status})

        self.dbi.processData(self.sql, bindVars, conn = conn,
                             transaction = transaction)
        return
