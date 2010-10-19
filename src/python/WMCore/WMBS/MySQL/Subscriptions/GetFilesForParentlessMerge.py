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
             FROM wmbs_sub_files_available
               INNER JOIN wmbs_file_details ON
                 wmbs_sub_files_available.file = wmbs_file_details.id
               INNER JOIN wmbs_file_runlumi_map ON
                 wmbs_file_details.id = wmbs_file_runlumi_map.file
               INNER JOIN wmbs_file_location ON
                 wmbs_file_details.id = wmbs_file_location.file
               INNER JOIN wmbs_location ON
                 wmbs_file_location.location = wmbs_location.id
             WHERE wmbs_sub_files_available.subscription = :p_1
             GROUP BY wmbs_file_details.id, wmbs_file_details.events,
                      wmbs_file_details.size, wmbs_file_details.lfn,
                      wmbs_file_details.first_event, wmbs_location.se_name"""

    def execute(self, subscription = None, conn = None, transaction = False):
        results = self.dbi.processData(self.sql, {"p_1": subscription}, conn = conn,
                                      transaction = transaction)
        return self.formatDict(results)
