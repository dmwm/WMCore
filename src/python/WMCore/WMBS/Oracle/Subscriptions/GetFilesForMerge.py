#!/usr/bin/env python
"""
_GetFilesForMerge_

Oracle implementation of Subscription.GetFilesForMerge
"""

__all__ = []
__revision__ = "$Id: GetFilesForMerge.py,v 1.7 2010/03/08 17:06:09 sfoulkes Exp $"
__version__ = "$Revision: 1.7 $"

from WMCore.WMBS.MySQL.Subscriptions.GetFilesForMerge import GetFilesForMerge as GetFilesForMergeMySQL

class GetFilesForMerge(GetFilesForMergeMySQL):
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
                    wmbs_file_runlumi_map.run AS file_run,
                    wmbs_file_runlumi_map.lumi AS file_lumi,
                    MIN(wmbs_file_parent.parent) AS file_parent
             FROM wmbs_file_details
             INNER JOIN wmbs_file_runlumi_map ON
               wmbs_file_details.id = wmbs_file_runlumi_map.fileid
             INNER JOIN
               (SELECT wmbs_fileset_files.fileid AS fileid FROM wmbs_fileset_files
                  INNER JOIN wmbs_subscription ON
                    wmbs_fileset_files.fileset = wmbs_subscription.fileset
                  LEFT OUTER JOIN wmbs_sub_files_acquired ON
                    wmbs_fileset_files.fileid = wmbs_sub_files_acquired.fileid AND
                    wmbs_sub_files_acquired.subscription = wmbs_subscription.id
                  LEFT OUTER JOIN wmbs_sub_files_complete ON
                    wmbs_fileset_files.fileid = wmbs_sub_files_complete.fileid AND
                    wmbs_sub_files_complete.subscription = wmbs_subscription.id
                  LEFT OUTER JOIN wmbs_sub_files_failed ON
                    wmbs_fileset_files.fileid = wmbs_sub_files_failed.fileid AND
                    wmbs_sub_files_failed.subscription = wmbs_subscription.id                    
                  WHERE wmbs_subscription.id = :p_1 AND
                        wmbs_sub_files_acquired.fileid IS NULL AND
                        wmbs_sub_files_complete.fileid IS NULL AND
                        wmbs_sub_files_failed.fileid IS NULL) merge_fileset ON
               wmbs_file_details.id = merge_fileset.fileid
             LEFT OUTER JOIN wmbs_file_parent ON
               wmbs_file_details.id = wmbs_file_parent.child
             WHERE wmbs_file_details.id NOT IN
               (SELECT child FROM wmbs_file_parent
                  INNER JOIN wmbs_job_assoc ON
                    wmbs_file_parent.parent = wmbs_job_assoc.fileid
                  INNER JOIN wmbs_job ON
                    wmbs_job_assoc.job = wmbs_job.id
                  INNER JOIN wmbs_jobgroup ON
                    wmbs_job.jobgroup = wmbs_jobgroup.id
                  INNER JOIN wmbs_job_state ON
                    wmbs_job.state = wmbs_job_state.id
                WHERE wmbs_job.outcome = 0 OR
                      wmbs_job_state.name != 'cleanout' AND
                      wmbs_jobgroup.subscription = :p_1)
             GROUP BY wmbs_file_details.id, wmbs_file_details.events,
                    wmbs_file_details.filesize, wmbs_file_details.lfn,
                    wmbs_file_details.first_event, wmbs_file_runlumi_map.run,
                    wmbs_file_runlumi_map.lumi"""
