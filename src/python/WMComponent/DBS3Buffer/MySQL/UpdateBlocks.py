#!/usr/bin/env python
"""
_DBS3Buffer.SetBlockStatus_

Create new block in dbsbuffer_block
Update file to reflect block information
"""

import threading

from WMCore.Database.DBFormatter import DBFormatter

class UpdateBlocks(DBFormatter):
    sql  = """UPDATE dbsbuffer_block SET status = :status, create_time = :time, location =
                (SELECT dbl.id FROM dbsbuffer_location dbl WHERE dbl.se_name = :location)
                WHERE blockname = :block"""

    sql3  = """UPDATE dbsbuffer_block SET status3 = :status WHERE blockname = :block"""

    def __init__(self, logger = None, dbi = None):
        myThread = threading.currentThread()
        DBFormatter.__init__(self, myThread.logger, myThread.dbi)


    def execute(self, blocks, dbs3UploadOnly = False, conn = None, transaction = False):
        """
        _execute_

        Changed to expect a DBSBlock object
        """
        bindVars = []

        for block in blocks:
            bindVars.append({"block": block.getName(), "location": block.getLocation(),
                             "status": block.status, "time": block.getStartTime()})

        if dbs3UploadOnly:
            self.dbi.processData(self.sql3, bindVars, conn = conn,
                                 transaction = transaction)
        else:
            self.dbi.processData(self.sql, bindVars, conn = conn,
                                 transaction = transaction)            

        return
