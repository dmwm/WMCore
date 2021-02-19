#!/usr/bin/env python
"""
_GetState_

MySQL implementation of Jobs.GetState
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

from future.utils import listvalues

class GetState(DBFormatter):
    """
    _GetState_

    Given a job ID, get the state of a current job.
    """
    sql = "SELECT name FROM wmbs_job_state WHERE id = (SELECT state FROM wmbs_job wj WHERE wj.ID = :jobid)"

    def format(self, results):
        """
        _formatDict_

        Return a name from MySQL
        """

        if len(results) == 0:
            return False
        else:
            return listvalues(results[0].fetchall()[0])[0]


    def execute(self, id, conn = None, transaction = False):
        """
        _execute_

        Execute the SQL for the given job ID and then format and return
        the result.
        """
        result = self.dbi.processData(self.sql, {"jobid": id}, conn = conn,
                                      transaction = transaction)

        return self.format(result)
