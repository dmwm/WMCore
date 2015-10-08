#!/usr/bin/env python
"""
_LoadForErrHandler_

Oracle implementation of Jobs.LoadForErrorHandler.
"""

from WMCore.WMBS.MySQL.Jobs.LoadForErrorHandler import LoadForErrorHandler as MySQLLoadForErrorHandler

class LoadForErrorHandler(MySQLLoadForErrorHandler):
    """
    _LoadForErrorHandler_

    If it's not the same as MySQL, I don't want to know.
    """

    fileSQL = """SELECT wfd.id, wfd.lfn, wfd.filesize \"size\", wfd.events, wfd.first_event,
                   wfd.merged, wja.job \"jobid\",
                   wls.se_name \"pnn\"
                 FROM wmbs_file_details wfd
                 INNER JOIN wmbs_job_assoc wja ON wja.fileid = wfd.id
                 INNER JOIN wmbs_file_location wfl ON wfl.fileid = wfd.id
                 INNER JOIN wmbs_location_senames wls ON wls.location = wfl.location
                 WHERE wja.job = :jobid"""
