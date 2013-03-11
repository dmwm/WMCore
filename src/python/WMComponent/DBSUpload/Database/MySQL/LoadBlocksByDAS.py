#!/usr/bin/env python
"""
_LoadBlocksByDAS_

MySQL implementation of LoadBlocksByDAS
"""




from WMCore.Database.DBFormatter import DBFormatter

class LoadBlocksByDAS(DBFormatter):
    sql = """SELECT dbb.blockname as blockname, dbb.create_time as create_time,
                (SELECT COUNT(*) FROM dbsbuffer_file dbf WHERE dbf.block_id = dbb.id) AS nFiles,
                (SELECT SUM(dbf2.filesize) FROM dbsbuffer_file dbf2 WHERE dbf2.block_id = dbb.id) AS blocksize,
                (SELECT SUM(dbf4.events) FROM dbsbuffer_file dbf4 WHERE dbf4.block_id = dbb.id) AS blockevents,
                dbl.se_name AS location, dbb.id AS id,
                dbw.block_close_max_wait_time, dbw.block_close_max_files,
                dbw.block_close_max_events, dbw.block_close_max_size
                FROM dbsbuffer_block dbb
                INNER JOIN dbsbuffer_file dbf3 ON dbf3.block_id = dbb.id
                INNER JOIN dbsbuffer_location dbl ON dbl.id = dbb.location
                INNER JOIN dbsbuffer_workflow dbw ON dbw.id = dbf3.workflow
                WHERE dbb.status = 'Open'
                AND dbf3.dataset_algo = :das
                GROUP BY dbb.blockname"""

    def format(self, result):
        tmpList = self.formatDict(result)
        blockList = []
        for tmp in tmpList:
            final = {}
            final['ID']            = tmp['id']
            final['Name']          = tmp['blockname']
            final['CreationDate']  = tmp['create_time']
            final['NumberOfFiles'] = tmp['nfiles']
            final['BlockSize']     = tmp['blocksize']
            final['NumberOfEvents'] = tmp['blockevents']
            final['location']      = tmp['location']
            final['newFiles']      = []
            final['insertedFiles'] = []
            final['open']          = 1
            final['MaxCloseTime'] = tmp['block_close_max_wait_time']
            final['MaxCloseEvents'] = tmp['block_close_max_events']
            final['MaxCloseFiles'] = tmp['block_close_max_files']
            final['MaxCloseSize'] = tmp['block_close_max_size']
            blockList.append(final)

        return blockList


    def execute(self, das, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {'das': das},
                         conn = conn, transaction = transaction)
        return self.format(result)
