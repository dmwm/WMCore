#!/usr/bin/env python
"""
_UpdateBlocks_

MySQL implementation of DBS3Buffer.UpdateBlocks
"""

from WMCore.Database.DBFormatter import DBFormatter

class UpdateBlocks(DBFormatter):
    sql  = """UPDATE dbsbuffer_block SET status = :status, create_time = :time, location =
                (SELECT dbl.id FROM dbsbuffer_location dbl WHERE dbl.pnn = :location)
                WHERE blockname = :block"""

    def execute(self, blocks, conn = None, transaction = False):
        bindVars = []

        for block in blocks:
            bindVars.append({"block": block.getName(), "location": block.getLocation(),
                             "status": block.status, "time": block.getStartTime()})
        self.dbi.processData(self.sql, bindVars, conn = conn,
                                 transaction = transaction)

        return
