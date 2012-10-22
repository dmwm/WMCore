#!/usr/bin/env python
"""
_GetBlock_

MySQL implementation of DBSBufferFiles.GetBlock
"""




from WMCore.Database.DBFormatter import DBFormatter

class GetOpenBlocks(DBFormatter):
    sql = """SELECT DISTINCT db1.blockname as blockname, db1.create_time as create_time, db1.status as status, dd.path as path
               FROM dbsbuffer_block db1
               INNER JOIN dbsbuffer_file df1 ON df1.block_id = db1.id
               INNER JOIN dbsbuffer_algo_dataset_assoc dada ON dada.id = df1.dataset_algo
               INNER JOIN dbsbuffer_dataset dd ON dd.id = dada.dataset_id
               LEFT OUTER JOIN dbsbuffer_file_parent dfp ON dfp.child = df1.id
               LEFT OUTER JOIN dbsbuffer_file df2 ON df2.id = dfp.parent
               LEFT OUTER JOIN dbsbuffer_block db2 ON db2.id = df2.block_id
             WHERE (db1.status = 'Open' OR db1.status = 'Pending')
             AND NOT EXISTS (SELECT df1.id FROM dbsbuffer_file df1
                             INNER JOIN dbsbuffer_file_parent dfp ON dfp.child = df1.id
                             INNER JOIN dbsbuffer_file df2 ON df2.id = dfp.parent
                             INNER JOIN dbsbuffer_block db2 ON db2.id = df2.block_id
                             WHERE df1.block_id = db1.id
                             AND (df2.status = 'NOTUPLOADED' OR (db2.status != 'Closed'
                                                                 AND db2.status != 'InGlobalDBS'
                                                                 AND db2.status IS NOT NULL)))
             """

    def execute(self, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {},
                         conn = conn, transaction = transaction)
        return self.formatDict(result)
