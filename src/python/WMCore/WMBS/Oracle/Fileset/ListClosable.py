#!/usr/bin/env python
"""
_ListClosable_

Oracle implementation of Fileset.ListClosable
"""




from WMCore.WMBS.MySQL.Fileset.ListClosable import ListClosable as ListFilesetClosableMySQL

class ListClosable(ListFilesetClosableMySQL):
    sql = """SELECT fileset FROM
               (SELECT wmbs_fileset.id AS fileset, SUM(wmbs_parent_fileset.open) AS open_parent_filesets,
                       SUM(fileset_size.total_files) AS total_input_files,
                       SUM(files_complete.total_files) AS total_complete_files,
                       SUM(files_failed.total_files) AS total_failed_files,
                       SUM(running_jobs.running_count) AS running_jobs FROM wmbs_fileset
                  INNER JOIN wmbs_workflow_output ON
                    wmbs_fileset.id = wmbs_workflow_output.output_fileset
                  INNER JOIN wmbs_subscription wmbs_parent_subscription ON
                    wmbs_workflow_output.workflow_id = wmbs_parent_subscription.workflow
                  LEFT OUTER JOIN (SELECT fileset, COUNT(fileid) AS total_files
                               FROM wmbs_fileset_files GROUP BY fileset) fileset_size ON
                    wmbs_parent_subscription.fileset = fileset_size.fileset
                  LEFT OUTER JOIN (SELECT subscription, COUNT(DISTINCT fileid) AS total_files
                              FROM wmbs_sub_files_complete GROUP BY subscription) files_complete ON
                    wmbs_parent_subscription.id = files_complete.subscription
                  LEFT OUTER JOIN (SELECT subscription, COUNT(DISTINCT fileid) AS total_files
                              FROM wmbs_sub_files_failed GROUP BY subscription) files_failed ON
                    wmbs_parent_subscription.id = files_failed.subscription
                  LEFT OUTER JOIN (SELECT subscription, COUNT(wmbs_job.id) AS running_count
                              FROM wmbs_jobgroup
                              LEFT OUTER JOIN wmbs_job ON
                                wmbs_jobgroup.id = wmbs_job.jobgroup
                              LEFT OUTER JOIN wmbs_job_state ON
                                wmbs_job.state = wmbs_job_state.id
                              WHERE wmbs_job_state.name != 'cleanout'
                              GROUP BY subscription) running_jobs ON
                    wmbs_parent_subscription.id = running_jobs.subscription                              
                  INNER JOIN wmbs_fileset wmbs_parent_fileset ON
                    wmbs_parent_subscription.fileset = wmbs_parent_fileset.id
                WHERE wmbs_fileset.open = 1 GROUP BY wmbs_fileset.id) closeable_filesets
             WHERE closeable_filesets.open_parent_filesets = 0 AND
                   COALESCE(closeable_filesets.running_jobs, 0) = 0 AND
                   COALESCE(closeable_filesets.total_input_files, 0) =
                     COALESCE(closeable_filesets.total_complete_files, 0) +
                     COALESCE(closeable_filesets.total_failed_files, 0)"""
