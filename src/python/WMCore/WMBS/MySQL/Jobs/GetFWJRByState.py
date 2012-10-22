#!/usr/bin/env python
"""
_GetFWJRByState_

MySQL implementation of Jobs.GetFWJRByState
"""




from WMCore.Database.DBFormatter import DBFormatter

class GetFWJRByState(DBFormatter):
    """
    _GetFWJRByState_

    Retrieve the ID and framework job report path of all jobs in a particular
    state.
    """
    sql = """SELECT id, fwjr_path FROM wmbs_job WHERE state =
               (SELECT id FROM wmbs_job_state WHERE name = :state)"""

    def format(self, results):
        """
        _format_

        """
        results = DBFormatter.format(self, results)

        jobs = []
        for result in results:
            jobs.append({"id": result[0], "fwjr_path": result[1]})

        return jobs

    def execute(self, state, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"state": state}, conn = conn,
                                      transaction = transaction)

        return self.format(result)
