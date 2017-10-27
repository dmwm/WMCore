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

    insertSQL = """INSERT INTO wmbs_file_location (fileid, pnn)
                     SELECT wfd.id, wpnn.id FROM wmbs_file_details wfd, wmbs_pnns wpnn
                     WHERE wfd.lfn = :lfn AND wpnn.pnn = :location
                     AND NOT EXISTS (SELECT fileid FROM wmbs_file_location
                                     WHERE fileid = wfd.id
                                     AND pnn=
                                     (SELECT id from wmbs_pnns where pnn=:location))"""
