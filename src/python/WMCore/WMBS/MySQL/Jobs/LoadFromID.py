#!/usr/bin/env python
"""
_LoadFromID_

MySQL implementation of Jobs.LoadFromID.
"""

__all__ = []
__revision__ = "$Id: LoadFromID.py,v 1.7 2009/09/10 16:18:07 mnorman Exp $"
__version__ = "$Revision: 1.7 $"

from WMCore.Database.DBFormatter import DBFormatter

class LoadFromID(DBFormatter):
    """
    _LoadFromID_

    Retrieve meta data for a job given it's ID.  This includes the name,
    job group and last update time.
    """
    sql = """SELECT wmbs_job.id, jobgroup, wmbs_job.name AS name, 
                    wmbs_job_state.name AS state, state_time, retry_count, 
                    couch_record,  cache_dir, wmbs_location.site_name AS location, 
                    outcome AS bool_outcome
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
        formattedResult = DBFormatter.formatDict(self, result)[0]

        if formattedResult["bool_outcome"] == 0:
            formattedResult["outcome"] = "fail"
        else:
            formattedResult["outcome"] = "success"

        del formattedResult["bool_outcome"]
        return formattedResult
    
    def execute(self, jobID, conn = None, transaction = False):
        """
        _execute_

        Execute the SQL for the given job ID and then format and return
        the result.
        """
        result = self.dbi.processData(self.sql, {"jobid": jobID}, conn = conn,
                                      transaction = transaction)
        return self.formatDict(result)
