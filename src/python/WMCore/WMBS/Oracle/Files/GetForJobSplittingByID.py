#!/usr/bin/env python

"""
_GetForJobSplittingByID_

Oracle implementation of File.GetForJobSplittingByID
"""
from WMCore.WMBS.MySQL.Files.GetForJobSplittingByID import GetForJobSplittingByID as MySQLGetByID

class GetForJobSplittingByID(MySQLGetByID):
    """
    Oracle version

    """

    sql = """SELECT id, lfn, filesize, events, first_event, merged, MIN(run) AS minrun
             FROM wmbs_file_details wfd
             LEFT OUTER JOIN wmbs_file_runlumi_map wfr ON wfr.fileid = wfd.id
             WHERE id = :fileid GROUP BY wfd.id, wfd.lfn, wfd.filesize, wfd.events, wfd.first_event,
             wfd.merged"""
