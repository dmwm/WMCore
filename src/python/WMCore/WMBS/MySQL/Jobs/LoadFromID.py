#!/usr/bin/env python
"""
_LoadFromID_

MySQL implementation of Jobs.LoadFromID.
"""

from WMCore.Database.DBFormatter import DBFormatter


class LoadFromID(DBFormatter):
    """
    _LoadFromID_

    Retrieve meta data for a job given it's ID.  This includes the name,
    job group and last update time.
    """
    sql = """SELECT wmbs_job.id, jobgroup, wmbs_job.name AS name,
                    wmbs_job_state.name AS state, wmbs_job.state_time, retry_count,
                    couch_record,  cache_dir, wmbs_location.site_name AS location,
                    outcome AS bool_outcome, fwjr_path AS fwjr_path
             FROM wmbs_job
               LEFT OUTER JOIN wmbs_location ON
                 wmbs_job.location = wmbs_location.id
               LEFT OUTER JOIN wmbs_job_state ON
                 wmbs_job.state = wmbs_job_state.id
             WHERE wmbs_job.id = :jobid"""

    def formatDict(self, result):
        """
        _formatDict_

        Cast the id, jobgroup and last_update columns to integers because
        formatDict() turns everything into strings.
        """

        formattedResult = DBFormatter.formatDict(self, result)

        for entry in formattedResult:
            if entry["bool_outcome"] == 0:
                entry["outcome"] = "failure"
            else:
                entry["outcome"] = "success"

            del entry["bool_outcome"]

        if len(formattedResult) == 1:
            return formattedResult[0]
        else:
            return formattedResult

    def execute(self, jobID, conn=None, transaction=False):
        """
        _execute_

        Execute the SQL for the given job ID and then format and return
        the result.
        """

        if isinstance(jobID, list):
            binds = jobID
        else:
            binds = {"jobid": jobID}

        result = self.dbi.processData(self.sql, binds, conn=conn,
                                      transaction=transaction)
        return self.formatDict(result)
