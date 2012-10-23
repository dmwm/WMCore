#!/usr/bin/env python
"""
_SetStatus_

MySQL implementation for altering job status
"""


from WMCore.Database.DBFormatter import DBFormatter

class SetStatus(DBFormatter):
    """
    _SetStatus_

    Set the status of a list of jobs
    """


    sql = """UPDATE bl_runjob SET sched_status =
               (SELECT id FROM bl_status WHERE name = :status)
               WHERE bl_runjob.id = :id"""


    def execute(self, jobs, status, conn = None, transaction = False):
        """
        _execute_

        Changes the status of a list of jobs to the given status
        Expects a list of IDs
        """

        if len(jobs) == 0:
            return

        binds = []
        for jobid in jobs:
            binds.append({'id': jobid, 'status': status})

        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)

        return
