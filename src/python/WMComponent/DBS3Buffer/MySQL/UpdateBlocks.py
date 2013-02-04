#!/usr/bin/env python
"""
_UpdateBlocks_

MySQL implementation of DBS3Buffer.UpdateBlocks
"""

from WMCore.Database.DBFormatter import DBFormatter

class UpdateBlocks(DBFormatter):
    sql  = """UPDATE dbsbuffer_block SET status = :status, create_time = :time, location =
                (SELECT dbl.id FROM dbsbuffer_location dbl WHERE dbl.se_name = :location)
                WHERE blockname = :block"""

    sql3  = """UPDATE dbsbuffer_block SET status3 = :status WHERE blockname = :block"""

    def execute(self, blocks, dbs3UploadOnly = False, conn = None, transaction = False):
        bindVars = []

        if dbs3UploadOnly:
            for block in blocks:
                bindVars.append({"block": block.getName(), "status": block.status})
            self.dbi.processData(self.sql3, bindVars, conn = conn,
                                 transaction = transaction)
        else:
            for block in blocks:
                bindVars.append({"block": block.getName(), "location": block.getLocation(),
                                 "status": block.status, "time": block.getStartTime()})            
            self.dbi.processData(self.sql, bindVars, conn = conn,
                                 transaction = transaction)            

        return
