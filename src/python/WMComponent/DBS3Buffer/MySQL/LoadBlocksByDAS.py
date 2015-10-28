#!/usr/bin/env python
"""
_LoadBlocksByDAS_

MySQL implementation of LoadBlocksByDAS
"""




from WMCore.Database.DBFormatter import DBFormatter

class LoadBlocksByDAS(DBFormatter):
    sql = """SELECT DISTINCT dbb.blockname as blockname, dbb.create_time as create_time,
                (SELECT COUNT(*) FROM dbsbuffer_file dbf WHERE dbf.block_id = dbb.id) AS nFiles,
                (SELECT SUM(dbf2.filesize) FROM dbsbuffer_file dbf2 WHERE dbf2.block_id = dbb.id) AS blocksize,
                dbl.pnn AS location
                FROM dbsbuffer_block dbb
                INNER JOIN dbsbuffer_file dbf3 ON dbf3.block_id = dbb.id
                INNER JOIN dbsbuffer_location dbl ON dbl.id = dbb.location
                WHERE dbb.status = 'Open'
                AND dbf3.dataset_algo = :das"""


    def format(self, result):
        tmpList = self.formatDict(result)
        blockList = []
        for tmp in tmpList:
            final = {}
            final['Name']          = tmp['blockname']
            final['CreationDate']  = tmp['create_time']
            final['NumberOfFiles'] = tmp['nfiles']
            final['BlockSize']     = float(tmp['blocksize'])
            final['location']      = tmp['location']
            final['newFiles']      = []
            final['insertedFiles'] = []
            final['open']          = 1
            blockList.append(final)

        return blockList


    def execute(self, das, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {'das': das},
                         conn = conn, transaction = transaction)
        return self.format(result)
