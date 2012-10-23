#!/usr/bin/env python
"""
_LoadBlocks_

MySQL implementation of LoadBlocks
"""




from WMCore.Database.DBFormatter import DBFormatter

class LoadBlocks(DBFormatter):
    sql = """SELECT DISTINCT dbb.blockname as blockname, dbb.create_time as create_time,
                (SELECT COUNT(*) FROM dbsbuffer_file dbf WHERE dbf.block_id = dbb.id) AS nFiles,
                (SELECT SUM(dbf2.filesize) FROM dbsbuffer_file dbf2 WHERE dbf2.block_id = dbb.id) AS blocksize,
                dbl.se_name AS location, dbf3.dataset_algo AS das
                FROM dbsbuffer_block dbb
                INNER JOIN dbsbuffer_file dbf3 ON dbf3.block_id = dbb.id
                INNER JOIN dbsbuffer_location dbl ON dbl.id = dbb.location
                WHERE dbb.blockname = :blockname"""


    def format(self, result):
        tmpList = self.formatDict(result)
        blockList = []
        for tmp in tmpList:
            final = {}
            final['block_name']       = tmp['blockname']
            final['creation_date']    = tmp['create_time']
            final['file_count']       = tmp['nfiles']
            final['block_size']       = tmp['blocksize']
            final['origin_site_name'] = tmp['location']
            final['DatasetAlgo']      = tmp['das']
            final['open_for_writing'] = 1
            blockList.append(final)

        return blockList


    def execute(self, blocknames, conn = None, transaction = False):
        """
        Take a list of blocknames and use them to load
        the blocks.
        """

        binds = []
        for name in blocknames:
            binds.append({'blockname': name})
        result = self.dbi.processData(self.sql, binds,
                                      conn = conn,
                                      transaction = transaction)
        return self.format(result)
