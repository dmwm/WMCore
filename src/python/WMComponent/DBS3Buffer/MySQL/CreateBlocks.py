#!/usr/bin/env python
"""
_DBSBuffer.SetBlockStatus_

Create new block in dbsbuffer_block
Update file to reflect block information
"""




import threading
import exceptions

from WMCore.Database.DBFormatter import DBFormatter

class CreateBlocks(DBFormatter):

    sql = """INSERT INTO dbsbuffer_block (blockname, location, status, create_time)
               SELECT :block, (SELECT id FROM dbsbuffer_location WHERE se_name = :location), :status, :time FROM DUAL
               """

    def execute(self, blocks, conn = None, transaction = False):
        """
        _execute_

        Changed to expect a DBSBlock object
        """
        bindVars = []

        for block in blocks:
            bindVars.append({"block": block.getName(), "location": block.getLocation(),
                             "status": block.status, "time": block.getStartTime()})


        self.dbi.processData(self.sql, bindVars, conn = conn,
                             transaction = transaction)


        return
