#!/usr/bin/env python
"""
_LoadBlocks_

MySQL implementation of LoadBlocks
"""

from WMCore.Database.DBFormatter import DBFormatter

class LoadBlocks(DBFormatter):
    sql = """SELECT DISTINCT dbb.blockname as blockname, dbb.create_time as create_time,
                dbb.status AS status, dbb.status3 AS status3,
                dbl.se_name AS location, dbf3.dataset_algo AS das,
                dbw.name AS workflow
              FROM dbsbuffer_block dbb
              INNER JOIN dbsbuffer_file dbf3 ON
                dbf3.block_id = dbb.id
              INNER JOIN dbsbuffer_location dbl ON
                dbl.id = dbb.location
              INNER JOIN dbsbuffer_workflow dbw ON
                dbw.id = dbf3.workflow
              WHERE dbb.blockname = :blockname"""

    def format(self, result, dbs3UploadOnly):
        tmpList = self.formatDict(result)
        blockList = []
        for tmp in tmpList:
            final = {}
            final['block_name']       = tmp['blockname']
            final['creation_date']    = tmp['create_time']
            final['origin_site_name'] = tmp['location']
            final['DatasetAlgo']      = tmp['das']
            final['workflow']      = tmp['workflow']
            
            if dbs3UploadOnly:
                final['status'] = tmp['status3']
            else:
                final['status'] = tmp['status']
                
            blockList.append(final)

        return blockList

    def execute(self, blocknames, dbs3UploadOnly, conn = None, transaction = False):
        """
        Take a list of blocknames and use them to load
        the blocks.
        """
        binds = []
        for name in blocknames:
            binds.append({'blockname': name})
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        return self.format(result, dbs3UploadOnly)
