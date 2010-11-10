#!/usr/bin/env python
"""
_AddToFileset_

Oracle implementation of Files.AddFileToFileset
"""

from WMCore.WMBS.MySQL.Files.AddToFileset import AddToFileset as AddFileToFilesetMySQL

class AddToFileset(AddFileToFilesetMySQL):
    sql = """INSERT INTO wmbs_fileset_files (fileid, fileset, insert_time)
               SELECT wmbs_file_details.id, :fileset, :insert_time
                      FROM wmbs_file_details
               WHERE wmbs_file_details.lfn = :lfn
               AND NOT EXISTS (SELECT fileid FROM wmbs_fileset_files wff2
                               WHERE wff2.fileid = wmbs_file_details.id AND
                                     wff2.fileset = :fileset)"""

    sqlAvail = """INSERT INTO wmbs_sub_files_available (subscription, fileid)
                    SELECT wmbs_subscription.id AS subscription,
                           wmbs_file_details.id AS fileid FROM wmbs_subscription
                      INNER JOIN wmbs_file_details ON
                        wmbs_file_details.lfn = :lfn
                    WHERE wmbs_subscription.fileset = :fileset AND NOT EXISTS
                      (SELECT * FROM wmbs_sub_files_available
                       WHERE fileid = (SELECT id FROM wmbs_file_details
                                     WHERE lfn = :lfn) AND
                             subscription = (SELECT id FROM wmbs_subscription
                                             WHERE fileset = :fileset AND rownum = 1))"""
