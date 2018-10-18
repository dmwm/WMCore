#!/usr/bin/env python
"""
_CountPhedexNotUploaded_

MySQL implementation of DBSBuffer.CountPhedexNotUploaded

"""

from __future__ import division
from WMCore.Database.DBFormatter import DBFormatter


class CountPhedexNotUploaded(DBFormatter):
    """
    CountPhedexNotUploaded

    Count the number of files in dbs not uploaded to phedex, used in drain statistics

    """
    sql = """SELECT COUNT(*) FROM dbsbuffer_file
             WHERE in_phedex=0
             AND block_id IS NOT NULL
             AND lfn NOT LIKE :unmerged
             AND lfn NOT LIKE :mcfakefile
             AND lfn NOT LIKE :backfill
             AND lfn NOT LIKE :storeuser"""

    def execute(self, conn=None, transaction=False):
        binds = {'unmerged': '%unmerged%',
                 'mcfakefile': 'MCFakeFile%',
                 'backfill': '%BACKFILL%',
                 'storeuser': '/store/user%'}

        result = self.dbi.processData(self.sql, binds, conn=conn,
                                      transaction=transaction)
        result = self.format(result)
        return result[0][0]
