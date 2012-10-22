#!/usr/bin/env python
"""
_DBSBuffer.SetBlockStatus_

Create new block in dbsbuffer_block
Update file to reflect block information
"""




import threading
import exceptions

from WMCore.Database.DBFormatter import DBFormatter

class UpdateBlocks(DBFormatter):

    sql  = """UPDATE dbsbuffer_block SET status = :status, create_time = :time, location =
                (SELECT dbl.id FROM dbsbuffer_location dbl WHERE dbl.se_name = :location)
                WHERE blockname = :block"""


    def __init__(self, logger = None, dbi = None):
        myThread = threading.currentThread()
        DBFormatter.__init__(self, myThread.logger, myThread.dbi)


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
