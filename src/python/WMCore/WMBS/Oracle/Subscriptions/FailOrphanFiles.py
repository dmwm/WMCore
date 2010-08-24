#!/usr/bin/env python
"""
_FailOrphanFiles_

Oracle implementation of Subscription.FailOrphanFiles
"""




from WMCore.WMBS.MySQL.Subscriptions.FailOrphanFiles import FailOrphanFiles as MySQLFailOrphanFiles

class FailOrphanFiles(MySQLFailOrphanFiles):
    sql = """INSERT INTO wmbs_sub_files_failed (subscription, fileid)
               SELECT DISTINCT :subscription, avail_files.fileid FROM
                 (SELECT wmbs_fileset_files.fileid AS fileid, :fileset AS fileset
                         FROM wmbs_fileset_files
                    LEFT OUTER JOIN wmbs_sub_files_acquired ON
                      wmbs_fileset_files.fileid = wmbs_sub_files_acquired.fileid AND
                      wmbs_sub_files_acquired.subscription = :subscription
                    LEFT OUTER JOIN wmbs_sub_files_complete ON
                      wmbs_fileset_files.fileid = wmbs_sub_files_complete.fileid AND
                      wmbs_sub_files_complete.subscription = :subscription
                    LEFT OUTER JOIN wmbs_sub_files_failed ON
                      wmbs_fileset_files.fileid = wmbs_sub_files_failed.fileid AND
                      wmbs_sub_files_failed.subscription = :subscription                 
                  WHERE wmbs_sub_files_acquired.fileid IS Null AND
                        wmbs_sub_files_complete.fileid IS Null AND
                        wmbs_sub_files_failed.fileid IS Null) avail_files
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
