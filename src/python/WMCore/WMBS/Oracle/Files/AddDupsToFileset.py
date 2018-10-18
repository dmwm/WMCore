#!/usr/bin/env python
"""
_AddDupsToFileset_

Oracle implementation of Files.AddDupsToFileset
"""

from WMCore.WMBS.MySQL.Files.AddDupsToFileset import AddDupsToFileset as MySQLAddDupsToFileset

class AddDupsToFileset(MySQLAddDupsToFileset):
    sql = """INSERT INTO wmbs_fileset_files (fileid, fileset, insert_time)
               SELECT wmbs_file_details.id, :fileset, :insert_time
               FROM wmbs_file_details
               WHERE wmbs_file_details.lfn = :lfn AND NOT EXISTS
                 (SELECT lfn FROM wmbs_file_details
                    INNER JOIN wmbs_fileset_files ON
                      wmbs_file_details.id = wmbs_fileset_files.fileid
                    INNER JOIN wmbs_subscription ON
                      wmbs_fileset_files.fileset = wmbs_subscription.fileset
                    INNER JOIN wmbs_workflow ON
                      wmbs_subscription.workflow = wmbs_workflow.id
                    WHERE wmbs_file_details.lfn = :lfn AND
                          wmbs_workflow.name = :workflow)
               AND NOT EXISTS (SELECT * FROM wmbs_fileset_files
                                WHERE fileset = :fileset
                                AND insert_time = :insert_time
                                AND fileid = wmbs_file_details.id)"""

    sqlAvail = """INSERT INTO wmbs_sub_files_available (subscription, fileid)
                    SELECT wmbs_subscription.id AS subscription,
                           wmbs_file_details.id AS fileid FROM wmbs_subscription
                      INNER JOIN wmbs_file_details ON
                        wmbs_file_details.lfn = :lfn
                    WHERE wmbs_subscription.fileset = :fileset AND NOT EXISTS
                 (SELECT lfn FROM wmbs_file_details
                    INNER JOIN wmbs_fileset_files ON
                      wmbs_file_details.id = wmbs_fileset_files.fileid
                    INNER JOIN wmbs_subscription ON
                      wmbs_fileset_files.fileset = wmbs_subscription.fileset
                    INNER JOIN wmbs_workflow ON
                      wmbs_subscription.workflow = wmbs_workflow.id
                    WHERE wmbs_file_details.lfn = :lfn AND
                          wmbs_workflow.name = :workflow AND
                          wmbs_fileset_files.fileset != :fileset)
                  AND NOT EXISTS (SELECT * FROM wmbs_sub_files_available
                                    WHERE subscription = wmbs_subscription.id
                                    AND fileid = wmbs_file_details.id)"""
