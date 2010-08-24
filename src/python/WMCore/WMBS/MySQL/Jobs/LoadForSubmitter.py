#!/usr/bin/env python
"""
_LoadForSubmitter_

MySQL function to load jobs for submission
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class LoadForSubmitter(DBFormatter):
    """
    _LoadForSubmitter_

    Custom load function for the JobSubmitter
    """
    sql = """SELECT wmbs_job.id, jobgroup, wmbs_job.name AS name, 
                    wmbs_job_state.name AS state, state_time, retry_count, 
                    couch_record,  cache_dir, wmbs_location.site_name AS location, 
                    outcome AS bool_outcome, fwjr_path AS fwjr_path
             FROM wmbs_job
               LEFT OUTER JOIN wmbs_location ON
                 wmbs_job.location = wmbs_location.id
             INNER JOIN wmbs_jobgroup ON wmbs_job.jobgroup = wmbs_jobgroup.id
             INNER JOIN wmbs_subscription ON wmbs_jobgroup.subscription = wmbs_subscription.id
             INNER JOIN wmbs_job_state ON wmbs_job.state = wmbs_job_state.id
             INNER JOIN wmbs_sub_types ON wmbs_subscription.subtype = wmbs_sub_types.id
             WHERE wmbs_job_state.name = 'created'
             AND wmbs_sub_types.name = :type
             """

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
    
    def execute(self, type, conn = None, transaction = False):
        """
        _execute_

        Execute the SQL for the given job type and then format and return
        the result.
        """

        binds = {"type": type}
            
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        return self.formatDict(result)
