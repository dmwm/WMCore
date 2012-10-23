#!/usr/bin/env python
"""
_FileCountBySubscription_

Monitoring DAO classes for Jobs in WMBS
TODO: this is not working version
"""
__all__ = []



from WMCore.WMBS.MySQL.Monitoring.DefaultFormatter import DefaultFormatter

class FileCountBySubscriptionAndRun(DefaultFormatter):
    """
    _FileCountBySubscription_

    count the number of files for each state for given subscription and run
    """

    sql = """SELECT COUNT(DISTINCT wmbs_sub_files_acquired.fileid) AS acquired
               ,COUNT(DISTINCT wmbs_sub_files_complete.fileid) AS complete
                ,COUNT(DISTINCT wmbs_sub_files_failed.fileid) AS failed
              FROM wmbs_file_details
              INNER JOIN wmbs_file_dataset_path_assoc ON wmbs_file_dataset_path_assoc.file_id = wmbs_file_details.id
              INNER JOIN dataset_path ON wmbs_file_dataset_path_assoc.dataset_path_id = dataset_path.id
              INNER JOIN data_tier ON dataset_path.data_tier = data_tier.id
              INNER JOIN wmbs_file_runlumi_map ON wmbs_file_details.id = wmbs_file_runlumi_map.fileid
              LEFT OUTER JOIN wmbs_sub_files_acquired ON wmbs_file_details.id = wmbs_sub_files_acquired.fileid
              LEFT OUTER JOIN wmbs_sub_files_complete ON wmbs_file_details.id = wmbs_sub_files_complete.fileid
              LEFT OUTER JOIN wmbs_sub_files_failed ON wmbs_file_details.id = wmbs_sub_files_failed.fileid
              WHERE run = :run
              AND (wmbs_sub_files_acquired.subscription = (SELECT id FROM wmbs_subscription WHERE workflow =
              (SELECT id FROM wmbs_workflow WHERE spec = 'FileMerge'))
              OR wmbs_sub_files_complete.subscription =
              (SELECT id FROM wmbs_subscription WHERE workflow =
              (SELECT id FROM wmbs_workflow WHERE spec = 'FileMerge'))
              OR wmbs_sub_files_failed.subscription =
              (SELECT id FROM wmbs_subscription WHERE workflow =
              (SELECT id FROM wmbs_workflow WHERE spec = 'FileMerge')))
               AND data_tier.name = 'RECO'
            """

    def execute(self, fileset_name, workflow_name, run, conn = None, transaction = False):
        """
        _execute_

        Execute the SQL for the given job ID and then format and return
        the result.
        """
        bindVars = {"fileset_name": fileset_name, "workflow_name": workflow_name, "run": run}

        result = self.dbi.processData(self.sql, bindVars, conn = conn,
                                      transaction = transaction)

        return self.formatDict(result)
