#!/usr/bin/env python

"""
SQLite implementation of AddChecksum
"""


__revision__ = "$Id: AddChecksum.py,v 1.1 2009/12/02 19:35:36 mnorman Exp $"
__version__  = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Files.AddChecksum import AddChecksum as MySQLAddChecksum

class AddChecksum(MySQLAddChecksum):
    """
    Adds a checksum

    """

    sql = """INSERT INTO wmbs_file_checksums (fileid, typeid, cksum)
             SELECT :fileid, (SELECT id FROM wmbs_checksum_type WHERE type = :cktype), :cksum 
             WHERE NOT EXISTS (SELECT fileid FROM wmbs_file_checksums WHERE
                               fileid = :fileid AND typeid = (SELECT id FROM wmbs_checksum_type WHERE type = :cktype))"""
