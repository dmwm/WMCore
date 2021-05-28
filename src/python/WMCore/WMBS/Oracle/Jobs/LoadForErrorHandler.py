#!/usr/bin/env python
"""
_LoadForErrorHandler_

Oracle implementation of Jobs.LoadForErrorHandler.
"""

from WMCore.WMBS.MySQL.Jobs.LoadForErrorHandler import LoadForErrorHandler as MySQLLoadForErrorHandler


class LoadForErrorHandler(MySQLLoadForErrorHandler):
    """
    The MySQL query should work, but no, it doesn't...
    """
    fileSQL = """SELECT wfd.id, wfd.lfn, wfd.filesize "size", wfd.events, wfd.first_event,
                   wfd.merged, wja.job "jobid", wpnn.pnn
                 FROM wmbs_file_details wfd
                   INNER JOIN wmbs_job_assoc wja ON wja.fileid = wfd.id
                   LEFT OUTER JOIN wmbs_file_location wfl ON wfd.id = wfl.fileid
                   LEFT OUTER JOIN wmbs_pnns wpnn ON wpnn.id = wfl.pnn
                 WHERE wja.job = :jobid"""

    parentSQL = """SELECT parent.lfn AS lfn, wfp.child AS id
                     FROM wmbs_file_parent wfp
                     INNER JOIN wmbs_file_details parent ON parent.id = wfp.parent
                     WHERE wfp.child = :fileid AND parent.merged = '1'"""
