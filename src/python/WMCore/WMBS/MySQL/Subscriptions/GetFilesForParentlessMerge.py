#!/usr/bin/env python
"""
_GetFilesForParentlessMerge_

MySQL implementation of Subscription.GetFilesForParentlessMerge
"""




from WMCore.Database.DBFormatter import DBFormatter

class GetFilesForParentlessMerge(DBFormatter):
    """
    This query needs to return the following for any files that is deemed
    mergeable:
      WMBS ID (file_id)
      Events (file_events)
      Size (file_size)
      LFN (file_lfn)
      First event in file (file_first_event)
      Runs in file (file_run)
      Lumi sections in file (file_lumi)
      Location
    """
    sql = """SELECT wmbs_file_details.id AS file_id,
                    wmbs_file_details.events AS file_events,
                    wmbs_file_details.size AS file_size,
                    wmbs_file_details.lfn AS file_lfn,
                    wmbs_file_details.first_event AS file_first_event,
                    MIN(wmbs_file_runlumi_map.run) AS file_run,
                    MIN(wmbs_file_runlumi_map.lumi) AS file_lumi,
                    wmbs_location.se_name AS se_name
             FROM wmbs_file_details
               INNER JOIN wmbs_file_runlumi_map ON
                 wmbs_file_details.id = wmbs_file_runlumi_map.file
               INNER JOIN wmbs_file_location ON
                 wmbs_file_details.id = wmbs_file_location.file
               INNER JOIN wmbs_location ON
                 wmbs_file_location.location = wmbs_location.id
               INNER JOIN wmbs_fileset_files ON
                 wmbs_file_details.id = wmbs_fileset_files.file
               INNER JOIN wmbs_subscription ON
                 wmbs_subscription.fileset = wmbs_fileset_files.fileset AND
                 wmbs_subscription.id = :p_1
               LEFT OUTER JOIN wmbs_sub_files_acquired ON
                 wmbs_fileset_files.file = wmbs_sub_files_acquired.file AND
                 wmbs_sub_files_acquired.subscription = :p_1
               LEFT OUTER JOIN wmbs_sub_files_complete ON
                 wmbs_fileset_files.file = wmbs_sub_files_complete.file AND
                 wmbs_sub_files_complete.subscription = :p_1
               LEFT OUTER JOIN wmbs_sub_files_failed ON
                 wmbs_fileset_files.file = wmbs_sub_files_failed.file AND
                 wmbs_sub_files_failed.subscription = :p_1
             WHERE wmbs_sub_files_acquired.file IS NULL AND
                   wmbs_sub_files_complete.file IS NULL AND
                   wmbs_sub_files_failed.file IS NULL
             GROUP BY wmbs_file_details.id, wmbs_file_details.events,
                      wmbs_file_details.size, wmbs_file_details.lfn,
                      wmbs_file_details.first_event, wmbs_location.se_name"""

    def execute(self, subscription = None, conn = None, transaction = False):
        results = self.dbi.processData(self.sql, {"p_1": subscription}, conn = conn,
                                      transaction = transaction)
        return self.formatDict(results)
