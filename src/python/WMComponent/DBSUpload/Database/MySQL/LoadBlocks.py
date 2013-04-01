#!/usr/bin/env python
"""
_GetBlock_

MySQL implementation of DBSBufferFiles.GetBlock
"""




from WMCore.Database.DBFormatter import DBFormatter

class LoadBlocks(DBFormatter):
    sql = """SELECT db1.blockname as blockname, db1.create_time as create_time, db1.status AS open,
                 db1.id AS id, dada.id AS das, dbw.block_close_max_wait_time
               FROM dbsbuffer_block db1
               INNER JOIN dbsbuffer_file df1 ON df1.block_id = db1.id
               INNER JOIN dbsbuffer_algo_dataset_assoc dada ON dada.id = df1.dataset_algo
               INNER JOIN dbsbuffer_workflow dbw ON df1.workflow = dbw.id
               WHERE db1.status = 'Open'
               OR db1.status = 'Pending'
               GROUP BY db1.blockname
             """

    def execute(self, conn = None, transaction = False):
        binds = {}
        result = self.dbi.processData(self.sql, binds,
                                      conn = conn, transaction = transaction)

        return self.formatDict(result)
