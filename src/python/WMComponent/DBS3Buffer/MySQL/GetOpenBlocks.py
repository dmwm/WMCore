#!/usr/bin/env python
"""
_GetOpenBlocks_

MySQL implementation of DBSBufferFiles.GetOpenBlocks
"""

from WMCore.Database.DBFormatter import DBFormatter

class GetOpenBlocks(DBFormatter):
    sql = """SELECT DISTINCT blockname AS blockname, create_time AS create_time
             FROM dbsbuffer_block
             WHERE status = 'Open' OR status = 'Pending'"""

    # Find only blocks that have been closed and uploaded to DBS2 but are still
    # waiting to be uploaded to DBS3.
    sql3 = """SELECT DISTINCT blockname AS blockname, create_time AS create_time
              FROM dbsbuffer_block
              WHERE status = 'InDBS' AND (status3 = 'Open' OR status3 = 'Pending')"""    

    def execute(self, dbs3OnlyUpload, conn = None, transaction = False):
        if dbs3OnlyUpload:
            result = self.dbi.processData(self.sql3, {}, conn = conn,
                                          transaction = transaction)
        else:
            result = self.dbi.processData(self.sql, {}, conn = conn,
                                          transaction = transaction)            
        return self.formatDict(result)
