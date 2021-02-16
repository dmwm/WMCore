#!/usr/bin/env python
"""
_GetCache_

MySQL implementation of Jobs.GetState
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

from future.utils import listvalues

class GetCache(DBFormatter):
    """
    _GetState_

    Given a job ID, get the state of a current job.
    """
    sql = "SELECT cache_dir FROM wmbs_job WHERE id = :jobid"

    def format(self, results):
        """
        _formatDict_

        Return a name from MySQL
        """

        if len(results) == 0:
            return False
        else:
            return listvalues(results[0].fetchall()[0])[0]


    def execute(self, ID, conn = None, transaction = False):
        """
        _execute_

        Execute the SQL for the given job ID and then format and return
        the result.
        """
        result = self.dbi.processData(self.sql, {"jobid": ID}, conn = conn,
                                      transaction = transaction)

        return self.format(result)
