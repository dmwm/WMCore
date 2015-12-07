#!/usr/bin/env python
"""
_ListRunningJobs_

Retrieve the name, time of last state change, couch record and current state for
all jobs that are in the following states:
  none
  new
  created
  submitted
  executing
"""




from WMCore.Database.DBFormatter import DBFormatter

class ListRunningJobs(DBFormatter):
    sql = """SELECT wmbs_job.name AS job_name,
                    wmbs_job.state_time AS timestamp,
                    wmbs_job.couch_record AS couch_record,
                    wmbs_job_state.name AS state FROM wmbs_job
               INNER JOIN wmbs_job_state ON
                 wmbs_job.state = wmbs_job_state.id
             WHERE wmbs_job_state.name = 'none' OR
                   wmbs_job_state.name = 'new' OR
                   wmbs_job_state.name = 'created' OR
                   wmbs_job_state.name = 'submitted' OR
                   wmbs_job_state.name = 'executing'"""

    def execute(self, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, conn = conn,
                                      transaction = transaction)
        return self.formatDict(result)
