#!/usr/bin/env python
"""
_FailOrphanFiles_

Oracle implementation of Subscriptions.FailOrphanFiles
"""

from WMCore.WMBS.MySQL.Subscriptions.FailOrphanFiles import FailOrphanFiles as MySQLFailOrphanFiles

class FailOrphanFiles(MySQLFailOrphanFiles):
    sql = """SELECT DISTINCT avail_files.fileid FROM
               (SELECT wmbs_sub_files_available.fileid AS fileid, :fileset AS fileset
                       FROM wmbs_sub_files_available
                WHERE wmbs_sub_files_available.subscription = :subscription) avail_files
               INNER JOIN wmbs_file_parent ON
                 avail_files.fileid = wmbs_file_parent.child
               INNER JOIN wmbs_job_assoc ON
                 wmbs_file_parent.parent = wmbs_job_assoc.fileid
               INNER JOIN wmbs_workflow_output ON
                 avail_files.fileset = wmbs_workflow_output.output_fileset
               INNER JOIN wmbs_subscription ON
                 wmbs_workflow_output.workflow_id = wmbs_subscription.workflow
               INNER JOIN wmbs_jobgroup ON
                 wmbs_subscription.id = wmbs_jobgroup.subscription
               INNER JOIN wmbs_job ON
                 wmbs_job_assoc.job = wmbs_job.id AND
                 wmbs_job.outcome = 0"""

    sqlFail = """INSERT INTO wmbs_sub_files_failed (subscription, fileid)
                   VALUES (:subscription, :fileid)"""

    sqlAvail = """DELETE FROM wmbs_sub_files_available
                  WHERE subscription = :subscription AND
                        fileid = :fileid"""
