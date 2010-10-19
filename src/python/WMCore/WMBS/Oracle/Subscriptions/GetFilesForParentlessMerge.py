#!/usr/bin/env python
"""
_GetFilesForParentlessMerge_

Oracle implementation of Subscription.GetFilesForParentlessMerge
"""

from WMCore.WMBS.MySQL.Subscriptions.GetFilesForParentlessMerge import GetFilesForParentlessMerge as GetFilesForParentlessMergeMySQL

class GetFilesForParentlessMerge(GetFilesForParentlessMergeMySQL):
    sql = """SELECT wmbs_file_details.id AS file_id,
                    wmbs_file_details.events AS file_events,
                    wmbs_file_details.filesize AS file_size,
                    wmbs_file_details.lfn AS file_lfn,
                    wmbs_file_details.first_event AS file_first_event,
                    MIN(wmbs_file_runlumi_map.run) AS file_run,
                    MIN(wmbs_file_runlumi_map.lumi) AS file_lumi,
                    wmbs_location.se_name AS se_name
             FROM wmbs_sub_files_available
               INNER JOIN wmbs_file_details ON
                 wmbs_sub_files_available.fileid = wmbs_file_details.id
               INNER JOIN wmbs_file_runlumi_map ON
                 wmbs_file_details.id = wmbs_file_runlumi_map.fileid
               INNER JOIN wmbs_file_location ON
                 wmbs_file_details.id = wmbs_file_location.fileid
               INNER JOIN wmbs_location ON
                 wmbs_file_location.location = wmbs_location.id
             WHERE wmbs_sub_files_available.subscription = :p_1
             GROUP BY wmbs_file_details.id, wmbs_file_details.events,
                      wmbs_file_details.filesize, wmbs_file_details.lfn,
                      wmbs_file_details.first_event, wmbs_location.se_name"""
