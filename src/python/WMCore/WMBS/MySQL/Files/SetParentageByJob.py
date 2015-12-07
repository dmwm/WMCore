#!/usr/bin/env python
"""
MySQL implementation of File.SetParentageByJob

Make the parentage link between a file and all the inputs of a given job
"""




import logging

from WMCore.Database.DBFormatter import DBFormatter

class SetParentageByJob(DBFormatter):
    """
    Make the parentage link between a file and all the inputs of a given job
    """


    sql = """INSERT INTO wmbs_file_parent (child, parent)
             SELECT DISTINCT wmbs_file_details.id, wmbs_job_assoc.fileid
             FROM wmbs_file_details, wmbs_job_assoc
             WHERE wmbs_job_assoc.job  = :jobid
             AND wmbs_file_details.lfn = :child
    """


    def execute(self, binds, conn = None, transaction = False):
        """
        Expect binds of form {'jobid', 'child'}

        """

        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)

        return
