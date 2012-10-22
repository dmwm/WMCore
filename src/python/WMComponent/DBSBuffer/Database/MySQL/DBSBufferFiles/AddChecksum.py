#!/usr/bin/env python

"""
MySQL implementation of AddChecksum
"""





from WMCore.Database.DBFormatter import DBFormatter

class AddChecksum(DBFormatter):
    sql = """INSERT INTO dbsbuffer_file_checksums (fileid, typeid, cksum)
             SELECT :fileid, (SELECT id FROM dbsbuffer_checksum_type WHERE type = :cktype), :cksum FROM dual
             WHERE NOT EXISTS (SELECT fileid FROM dbsbuffer_file_checksums WHERE
                               fileid = :fileid AND typeid = (SELECT id FROM dbsbuffer_checksum_type WHERE type = :cktype))"""

    def execute(self, fileid = None, cktype = None, cksum = None, bulkList = None, conn = None,
                transaction = False):

        if bulkList:
            binds = bulkList
        else:
            binds = {'fileid': fileid, 'cktype': cktype, 'cksum': cksum}

        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)

        return
