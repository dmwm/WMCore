#!/usr/bin/env python
"""
_SetLocationForWorkQueue_

Oracle implementation of Files.SetLocationForWorkQueue

For WorkQueue only
"""

from WMCore.WMBS.MySQL.Files.SetLocationForWorkQueue import SetLocationForWorkQueue as MySQLSetLocationForWorkQueue

class SetLocationForWorkQueue(MySQLSetLocationForWorkQueue):
    """
    Oracle version


    """

    insertSQL = """INSERT INTO wmbs_file_location (fileid, location)
                     SELECT wfd.id, :location FROM wmbs_file_details wfd
                       WHERE wfd.lfn = :lfn
                       AND NOT EXISTS (SELECT fileid FROM wmbs_file_location
                                        WHERE fileid = wfd.id
                                        AND location = :location)"""
