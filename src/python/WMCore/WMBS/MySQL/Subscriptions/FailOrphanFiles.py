#!/usr/bin/env python
"""
_FailOrphanFiles_

MySQL implementation of Subscription.FailOrphanFiles
"""




from WMCore.Database.DBFormatter import DBFormatter

class FailOrphanFiles(DBFormatter):
    sql = """INSERT INTO wmbs_sub_files_failed (subscription, file)
               SELECT DISTINCT :subscription, avail_files.fileid FROM
                 (SELECT wmbs_fileset_files.file AS fileid, wmbs_fileset_files.fileset AS fileset
                         FROM wmbs_fileset_files
                    LEFT OUTER JOIN wmbs_sub_files_acquired ON
                      wmbs_fileset_files.file = wmbs_sub_files_acquired.file AND
                      wmbs_sub_files_acquired.subscription = :subscription
                    LEFT OUTER JOIN wmbs_sub_files_complete ON
                      wmbs_fileset_files.file = wmbs_sub_files_complete.file AND
                      wmbs_sub_files_complete.subscription = :subscription
                    LEFT OUTER JOIN wmbs_sub_files_failed ON
                      wmbs_fileset_files.file = wmbs_sub_files_failed.file AND
                      wmbs_sub_files_failed.subscription = :subscription                 
                  WHERE wmbs_fileset_files.fileset = :fileset AND
                        wmbs_sub_files_acquired.file IS Null AND
                        wmbs_sub_files_complete.file IS Null AND
                        wmbs_sub_files_failed.file IS Null) avail_files
                 INNER JOIN wmbs_file_parent ON
                   avail_files.fileid = wmbs_file_parent.child
                 INNER JOIN wmbs_job_assoc ON
                   wmbs_file_parent.parent = wmbs_job_assoc.file
                 INNER JOIN wmbs_workflow_output ON
                   avail_files.fileset = wmbs_workflow_output.output_fileset
                 INNER JOIN wmbs_subscription ON
                   wmbs_workflow_output.workflow_id = wmbs_subscription.workflow
                 INNER JOIN wmbs_jobgroup ON
                   wmbs_subscription.id = wmbs_jobgroup.subscription
                 INNER JOIN wmbs_job ON
                   wmbs_jobgroup.id = wmbs_job.jobgroup AND
                   wmbs_job_assoc.job = wmbs_job.id AND
                   wmbs_job.outcome = 0"""
    
    def execute(self, subscriptionID, filesetID, conn = None, transaction = False):
        binds = {"subscription": subscriptionID, "fileset": filesetID}
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
