#!/usr/bin/env python

"""
MySQL implementation of AddChecksumByLFN
"""





from WMCore.Database.DBFormatter import DBFormatter

class AddChecksumByLFN(DBFormatter):
    sql = """INSERT INTO wmbs_file_checksums (fileid, typeid, cksum)
             SELECT (SELECT id FROM wmbs_file_details WHERE lfn = :lfn),
              (SELECT id FROM wmbs_checksum_type WHERE type = :cktype),
              :cksum FROM dual
             WHERE NOT EXISTS (SELECT fileid FROM wmbs_file_checksums WHERE
                               fileid = (SELECT id FROM wmbs_file_details WHERE lfn = :lfn)
                               AND typeid = (SELECT id FROM wmbs_checksum_type WHERE type = :cktype))"""
                
    def execute(self, lfn = None, cktype = None, cksum = None, bulkList = None, conn = None,
                transaction = False):

        if bulkList:
            binds = bulkList
        else:
            binds = {'lfn': lfn, 'cktype': cktype, 'cksum': cksum}

        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)

        return
