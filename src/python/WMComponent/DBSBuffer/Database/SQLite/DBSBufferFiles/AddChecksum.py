#!/usr/bin/env python

"""
SQLite implementation of AddChecksum
"""





from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.AddChecksum import AddChecksum as MySQLAddChecksum

class AddChecksum(MySQLAddChecksum):
    """
    Add a Checksum

    """

    sql = """INSERT INTO dbsbuffer_file_checksums (fileid, typeid, cksum)
             SELECT :fileid, (SELECT id FROM dbsbuffer_checksum_type WHERE type = :cktype), :cksum 
             WHERE NOT EXISTS (SELECT fileid FROM dbsbuffer_file_checksums WHERE
                               fileid = :fileid AND typeid = (SELECT id FROM dbsbuffer_checksum_type WHERE type = :cktype))"""
