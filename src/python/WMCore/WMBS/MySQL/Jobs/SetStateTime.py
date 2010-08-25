#!/usr/bin/env python
"""
_SetStateTime_

MySQL implementation of Jobs.SetStateTime
"""




from WMCore.Database.DBFormatter import DBFormatter

class SetStateTime(DBFormatter):
    """
    _SetStateTime_

    Update the state transition time for the given job.
    """
    sql = "UPDATE wmbs_job SET state_time = :statetime WHERE id = :jobid"

    def execute(self, jobID = None, stateTime = None, conn = None,
                transaction = False):
        binds = {"jobid": jobID, "statetime": stateTime}
        self.dbi.processData(self.sql, binds,
                             conn = conn, transaction = transaction)
        return
