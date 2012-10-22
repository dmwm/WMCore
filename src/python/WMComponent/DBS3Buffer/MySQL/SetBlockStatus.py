#!/usr/bin/env python
"""
_DBSBuffer.SetBlockStatus_

Create new block in dbsbuffer_block
Update file to reflect block information
"""




import threading
import exceptions

from WMCore.Database.DBFormatter import DBFormatter

class SetBlockStatus(DBFormatter):

    existsSQL = """
    SELECT blockname FROM dbsbuffer_block WHERE blockname = :block
    """

    sql = """INSERT INTO dbsbuffer_block (blockname, location, status, create_time)
               SELECT :block, (SELECT id FROM dbsbuffer_location WHERE se_name = :location), :status, :time FROM DUAL
               """

    updateSQL  = """UPDATE dbsbuffer_block SET status = :status, create_time = :time, location =
                      (SELECT id FROM dbsbuffer_location WHERE se_name = :location)
                      WHERE blockname = :block
    """


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



        testResult = self.dbi.processData(self.existsSQL, {'block': block}, conn = conn, transaction = transaction)

        res1 = self.formatDict(testResult)

        if res1 == []:
            sql = self.sql
        else:
            sql = self.updateSQL



        self.dbi.processData(sql, bindVars, conn = conn,
                                     transaction = transaction)


        return
