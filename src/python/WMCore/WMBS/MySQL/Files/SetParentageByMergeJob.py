#!/usr/bin/env python
"""
MySQL implementation of File.SetParentageByMergeJob

Make the parentage link between a file and all the inputs of a given job
"""




import logging

from WMCore.Database.DBFormatter import DBFormatter

class SetParentageByMergeJob(DBFormatter):
    """
    Make the parentage link between a file and all the inputs of a given job
    """


    sql = """INSERT INTO wmbs_file_parent (child, parent)
             SELECT DISTINCT wmbs_file_details.id, wmbs_job_assoc.fileid
             FROM wmbs_file_details, wmbs_job_assoc
             INNER JOIN wmbs_job ON
                 wmbs_job_assoc.job = wmbs_job.id
             INNER JOIN wmbs_jobgroup ON
                 wmbs_jobgroup.id = wmbs_job.jobgroup
             LEFT OUTER JOIN wmbs_sub_files_failed ON
                 wmbs_jobgroup.subscription = wmbs_sub_files_failed.subscription
                 AND wmbs_sub_files_failed.fileid = wmbs_job_assoc.fileid
             WHERE wmbs_job_assoc.job  = :jobid
             AND wmbs_file_details.lfn = :child
             AND wmbs_sub_files_failed.fileid IS NULL
    """


    def execute(self, binds, conn = None, transaction = False):
        """
        Expect binds of form {'jobid', 'child'}

        """

        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)

        return
