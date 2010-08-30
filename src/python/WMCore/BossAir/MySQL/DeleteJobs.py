#!/usr/bin/env python
"""
_DeleteJobs_

MySQL implementation for creating a deleting a job
"""


from WMCore.Database.DBFormatter import DBFormatter

class DeleteJobs(DBFormatter):
    """
    _DeleteJobs_

    Delete jobs from bl_runjob
    """


    sql = """DELETE FROM bl_runjob WHERE id = :id
    """
    def execute(self, jobs, conn = None, transaction = False):
        """
        _execute_

        Delete jobs
        Expects a list of IDs
        """

        if len(jobs) == 0:
            return

        binds = []
        for jobid in jobs:
            binds.append({'id': jobid})

        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)

        return
