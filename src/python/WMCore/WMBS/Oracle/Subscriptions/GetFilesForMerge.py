#!/usr/bin/env python
"""
_GetFilesForMerge_

Oracle implementation of Subscription.GetFilesForMerge
"""




from WMCore.WMBS.MySQL.Subscriptions.GetFilesForMerge import GetFilesForMerge as GetFilesForMergeMySQL

class GetFilesForMerge(GetFilesForMergeMySQL):
    """
    This query only differs from the MySQL version in the names of columns:
      file -> fileid
      size -> filesize

    See the MySQL version of this object for a narrative on this query.
    """
    sql = """SELECT merge_files.fileid AS file_id,
                    merge_files.parent AS file_parent,
                    wmbs_file_details.events AS file_events,
                    wmbs_file_details.filesize AS file_size,
                    wmbs_file_details.lfn AS file_lfn,
                    wmbs_file_details.first_event AS file_first_event,
                    MIN(wmbs_file_runlumi_map.run) AS file_run,
                    MIN(wmbs_file_runlumi_map.lumi) AS file_lumi,
                    wmbs_location.se_name AS se_name       
             FROM (
               SELECT wmbs_fileset_files.fileid AS fileid,
                      MIN(wmbs_file_parent.parent) AS parent,
                      COUNT(wmbs_fileset_files.fileid),
                      COUNT(b.id)
               FROM wmbs_fileset_files
               INNER JOIN wmbs_subscription ON
                 wmbs_subscription.fileset = wmbs_fileset_files.fileset AND
                 wmbs_subscription.id = :p_1
               LEFT OUTER JOIN wmbs_sub_files_acquired ON
                 wmbs_fileset_files.fileid = wmbs_sub_files_acquired.fileid AND
                 wmbs_sub_files_acquired.subscription = :p_1
               LEFT OUTER JOIN wmbs_sub_files_complete ON
                 wmbs_fileset_files.fileid = wmbs_sub_files_complete.fileid AND
                 wmbs_sub_files_complete.subscription = :p_1
               LEFT OUTER JOIN wmbs_sub_files_failed ON
                 wmbs_fileset_files.fileid = wmbs_sub_files_failed.fileid AND
                 wmbs_sub_files_failed.subscription = :p_1
               INNER JOIN wmbs_file_parent ON
                 wmbs_file_parent.child = wmbs_fileset_files.fileid
               INNER JOIN wmbs_job_assoc ON
                 wmbs_file_parent.parent = wmbs_job_assoc.fileid
               INNER JOIN wmbs_workflow_output ON
                 wmbs_fileset_files.fileset = wmbs_workflow_output.output_fileset
               INNER JOIN wmbs_subscription c ON
                 wmbs_workflow_output.workflow_id = c.workflow
               INNER JOIN wmbs_jobgroup ON
                 c.id = wmbs_jobgroup.subscription
               INNER JOIN wmbs_job a ON
                 a.id = wmbs_job_assoc.job AND
                 a.jobgroup = wmbs_jobgroup.id
               LEFT OUTER JOIN wmbs_job b ON
                 b.id = wmbs_job_assoc.job AND
                 b.outcome = 1
               WHERE wmbs_sub_files_acquired.fileid IS NULL AND
                     wmbs_sub_files_complete.fileid IS NULL AND
                     wmbs_sub_files_failed.fileid IS NULL
               GROUP BY wmbs_fileset_files.fileid, a.jobgroup
               HAVING COUNT(wmbs_fileset_files.fileid) = COUNT(b.id)) merge_files
             INNER JOIN wmbs_file_details ON
               wmbs_file_details.id = merge_files.fileid
             INNER JOIN wmbs_file_runlumi_map ON
               wmbs_file_runlumi_map.fileid = merge_files.fileid
             INNER JOIN wmbs_file_location ON
               wmbs_file_details.id = wmbs_file_location.fileid
             INNER JOIN wmbs_location ON
               wmbs_file_location.location = wmbs_location.id               
             GROUP BY merge_files.fileid, merge_files.parent,
                      wmbs_file_details.events, wmbs_file_details.filesize,
                      wmbs_file_details.lfn, wmbs_file_details.first_event,
                      wmbs_location.se_name"""
