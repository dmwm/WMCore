#!/usr/bin/env python
"""
_AddChecksumByLFN_

SQLite implementation of AddChecksumByLFN
"""

__revision__ = "$Id: AddChecksumByLFN.py,v 1.2 2010/04/08 20:09:09 sfoulkes Exp $"
__version__  = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Files.AddChecksumByLFN import AddChecksumByLFN as MySQLAddChecksumByLFN

class AddChecksumByLFN(MySQLAddChecksumByLFN):
    sql = """INSERT INTO wmbs_file_checksums (fileid, typeid, cksum)
             SELECT (SELECT id FROM wmbs_file_details WHERE lfn = :lfn),
              (SELECT id FROM wmbs_checksum_type WHERE type = :cktype),
              :cksum
             WHERE NOT EXISTS (SELECT fileid FROM wmbs_file_checksums WHERE
                               fileid = (SELECT id FROM wmbs_file_details WHERE lfn = :lfn)
                               AND typeid = (SELECT id FROM wmbs_checksum_type WHERE type = :cktype))"""
