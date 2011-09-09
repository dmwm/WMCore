#!/usr/bin/env python

"""
Oracle implementation of AddChecksumByLFN
"""





from WMComponent.DBS3Buffer.MySQL.DBSBufferFiles.AddChecksumByLFN import AddChecksumByLFN as MySQLAddChecksumByLFN

class AddChecksumByLFN(MySQLAddChecksumByLFN):
    """

    Add Checksums using lfn as key

    """

    sql = """INSERT INTO dbsbuffer_file_checksums (fileid, typeid, cksum)
             SELECT (SELECT id FROM dbsbuffer_file WHERE lfn = :lfn),
             (SELECT id FROM dbsbuffer_checksum_type WHERE type = :cktype), :cksum FROM dual
             WHERE NOT EXISTS (SELECT fileid FROM dbsbuffer_file_checksums WHERE
                               fileid = (SELECT id FROM dbsbuffer_file WHERE lfn = :lfn)
                               AND typeid = (SELECT id FROM dbsbuffer_checksum_type WHERE type = :cktype))"""
