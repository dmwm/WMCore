#!/usr/bin/env python
"""
_GetOpenBlocks_

MySQL implementation of DBS3Buffer.GetOpenBlocks
"""

from WMCore.Database.DBFormatter import DBFormatter

class GetOpenBlocks(DBFormatter):
    sql = """SELECT dbsbuffer_block.blockname AS blockname, dbsbuffer_block.create_time AS create_time,
                    pending_parents.parent_count FROM dbsbuffer_block
               LEFT OUTER JOIN
                 (SELECT dbsbuffer_block.id AS block_id, COUNT(dbsbuffer_block.id) AS parent_count FROM dbsbuffer_block
                    INNER JOIN dbsbuffer_file ON
                      dbsbuffer_block.id = dbsbuffer_file.block_id
                    LEFT OUTER JOIN dbsbuffer_file_parent ON
                      dbsbuffer_file.id = dbsbuffer_file_parent.child
                    LEFT OUTER JOIN dbsbuffer_file dbsbuffer_file_parent_detail ON
                       dbsbuffer_file_parent.parent = dbsbuffer_file_parent_detail.id
                    LEFT OUTER JOIN dbsbuffer_block dbsbuffer_block_parent ON
                      dbsbuffer_file_parent_detail.block_id = dbsbuffer_block_parent.id
                    WHERE dbsbuffer_block_parent.status3 = 'Pending'
                    GROUP BY dbsbuffer_block.id) pending_parents ON
                   dbsbuffer_block.id = pending_parents.block_id
             WHERE dbsbuffer_block.status = 'Open' OR dbsbuffer_block.status = 'Pending' AND
                   (pending_parents.parent_count IS NULL or pending_parents.parent_count = 0)"""    

    # Find only blocks that have been closed and uploaded to DBS2 but are still
    # waiting to be uploaded to DBS3.
    sql3 = """SELECT dbsbuffer_block.blockname AS blockname, dbsbuffer_block.create_time AS create_time,
                     pending_parents.parent_count FROM dbsbuffer_block
                LEFT OUTER JOIN
                  (SELECT dbsbuffer_block.id AS block_id, COUNT(dbsbuffer_block.id) AS parent_count FROM dbsbuffer_block
                     INNER JOIN dbsbuffer_file ON
                       dbsbuffer_block.id = dbsbuffer_file.block_id
                     LEFT OUTER JOIN dbsbuffer_file_parent ON
                       dbsbuffer_file.id = dbsbuffer_file_parent.child
                     LEFT OUTER JOIN dbsbuffer_file dbsbuffer_file_parent_detail ON
                        dbsbuffer_file_parent.parent = dbsbuffer_file_parent_detail.id
                     LEFT OUTER JOIN dbsbuffer_block dbsbuffer_block_parent ON
                       dbsbuffer_file_parent_detail.block_id = dbsbuffer_block_parent.id
                     WHERE dbsbuffer_block_parent.status3 = 'Pending'
                     GROUP BY dbsbuffer_block.id) pending_parents ON
                    dbsbuffer_block.id = pending_parents.block_id
              WHERE (dbsbuffer_block.status = 'Closed' OR dbsbuffer_block.status = 'InGlobalDBS') AND
                    (dbsbuffer_block.status3 = 'Pending' OR dbsbuffer_block.status3 = 'Open') AND
                    (pending_parents.parent_count IS NULL or pending_parents.parent_count = 0)"""

    def execute(self, dbs3OnlyUpload, conn = None, transaction = False):
        if dbs3OnlyUpload:
            result = self.dbi.processData(self.sql3, {}, conn = conn,
                                          transaction = transaction)
        else:
            result = self.dbi.processData(self.sql, {}, conn = conn,
                                          transaction = transaction)            
        return self.formatDict(result)
