"""
_WorkUnit.GetState_

MySQL implementation of WorkUnit.GetState
"""

from __future__ import absolute_import, division, print_function

from WMCore.Database.DBFormatter import DBFormatter


class GetState(DBFormatter):
    """
    _GetState_

    Given a workunit ID, get the state of the workunit
    """
    sql = "SELECT status FROM wmbs_workunit WHERE id = :wuid"

    def format(self, results):
        """
        _formatDict_

        Return a name from MySQL
        """

        if len(results) == 0:
            return False
        else:
            return results[0].fetchall()[0].values()[0]

    def execute(self, wuid, conn=None, transaction=False):
        """
        _execute_

        Execute the SQL for the given job ID and then format and return
        the result.
        """

        result = self.dbi.processData(self.sql, {"wuid": wuid}, conn=conn, transaction=transaction)

        return self.format(result)
