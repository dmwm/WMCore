#!/usr/bin/env python
"""
_LoadBlocks_

MySQL implementation of LoadBlocks
"""

from WMCore.Database.DBFormatter import DBFormatter

class LoadBlocks(DBFormatter):

    sql = """SELECT dbsbuffer_block.blockname as blockname,
                    dbsbuffer_block.create_time as create_time,
                    dbsbuffer_block.status AS status,
                    dbsbuffer_dataset.path AS datasetpath,
                    dbsbuffer_location.pnn AS location
              FROM dbsbuffer_block
              INNER JOIN dbsbuffer_dataset ON
                dbsbuffer_dataset.id = dbsbuffer_block.dataset_id
              INNER JOIN dbsbuffer_location ON
                dbsbuffer_location.id = dbsbuffer_block.location
              WHERE dbsbuffer_block.blockname = :blockname
              """

    def format(self, result):

        tmpList = self.formatDict(result)
        blockList = []
        for tmp in tmpList:
            blockList.append( { 'block_name' : tmp['blockname'],
                                'creation_date' : tmp['create_time'],
                                'origin_site_name' : tmp['location'],
                                'datasetpath' : tmp['datasetpath'],
                                'status' : tmp['status'] } )

        return blockList

    def execute(self, blocknames, conn = None, transaction = False):
        """
        Take a list of blocknames and use them to load
        the blocks.
        """
        binds = []
        for name in blocknames:
            binds.append({'blockname': name})
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        return self.format(result)
