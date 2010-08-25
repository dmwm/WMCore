#!/usr/bin/env python
"""
_NewestStateChangeForSub_

MySQL implementation of Jobs.NewestStateChangeForSub
"""




from WMCore.Database.DBFormatter import DBFormatter

class NewestStateChangeForSub(DBFormatter):
    sql = """SELECT wmbs_job.state_time, wmbs_job_state.name FROM wmbs_job
               INNER JOIN wmbs_job_state
                 ON wmbs_job.state = wmbs_job_state.id
             WHERE state_time =
               (SELECT MAX(state_time) FROM wmbs_job
                  INNER JOIN wmbs_jobgroup
                    ON wmbs_job.jobgroup = wmbs_jobgroup.id
                WHERE wmbs_jobgroup.subscription = :sub)"""
    
    def execute(self, subscription, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"sub": subscription},
                                      conn = conn, transaction = transaction)
        return self.formatDict(result)
