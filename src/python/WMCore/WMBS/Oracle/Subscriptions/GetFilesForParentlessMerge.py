#!/usr/bin/env python
"""
_GetFilesForParentlessMerge_

Oracle implementation of Subscription.GetFilesForParentlessMerge
"""




from WMCore.WMBS.MySQL.Subscriptions.GetFilesForParentlessMerge import GetFilesForParentlessMerge as GetFilesForParentlessMergeMySQL

class GetFilesForParentlessMerge(GetFilesForParentlessMergeMySQL):
    """
    This query only differs from the MySQL version in the names of columns:
      file -> fileid
      size -> filesize

    See the MySQL version of this object for a narrative on this query.
    """
    sql = """SELECT wmbs_file_details.id AS file_id,
                    wmbs_file_details.events AS file_events,
                    wmbs_file_details.filesize AS file_size,
                    wmbs_file_details.lfn AS file_lfn,
                    wmbs_file_details.first_event AS file_first_event,
                    MIN(wmbs_file_runlumi_map.run) AS file_run,
                    MIN(wmbs_file_runlumi_map.lumi) AS file_lumi,
                    wmbs_location.se_name AS se_name
             FROM wmbs_file_details
               INNER JOIN wmbs_file_runlumi_map ON
                 wmbs_file_details.id = wmbs_file_runlumi_map.fileid
               INNER JOIN wmbs_file_location ON
                 wmbs_file_details.id = wmbs_file_location.fileid
               INNER JOIN wmbs_location ON
                 wmbs_file_location.location = wmbs_location.id
               INNER JOIN wmbs_fileset_files ON
                 wmbs_file_details.id = wmbs_fileset_files.fileid
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
             WHERE wmbs_sub_files_acquired.fileid IS NULL AND
                   wmbs_sub_files_complete.fileid IS NULL AND
                   wmbs_sub_files_failed.fileid IS NULL
             GROUP BY wmbs_file_details.id, wmbs_file_details.events,
                      wmbs_file_details.filesize, wmbs_file_details.lfn,
                      wmbs_file_details.first_event, wmbs_location.se_name"""
