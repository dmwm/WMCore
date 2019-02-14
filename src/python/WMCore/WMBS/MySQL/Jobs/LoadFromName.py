#!/usr/bin/env python
"""
_LoadFromName_

MySQL implementation of Jobs.LoadFromName.
"""

__all__ = []

from WMCore.Database.DBFormatter import DBFormatter


class LoadFromName(DBFormatter):
    """
    _LoadFromName_

    Retrieve meta data for a job given it's name.  This includes the name,
    job group and last update time.
    """
    sql = """SELECT wmbs_job.id, jobgroup, wmbs_job.name AS name,
                    wmbs_job_state.name AS state, wmbs_job.state_time, retry_count,
                    couch_record, cache_dir, wmbs_location.site_name AS location,
                    outcome AS bool_outcome, fwjr_path AS fwjr_path
             FROM wmbs_job
               LEFT OUTER JOIN wmbs_location ON
                 wmbs_job.location = wmbs_location.id
               LEFT OUTER JOIN wmbs_job_state ON
                 wmbs_job.state = wmbs_job_state.id
             WHERE wmbs_job.name = :name"""

    def formatDict(self, result):
        """
        _formatDict_

        Cast the id, jobgroup and last_update columns to integers because
        formatDict() turns everything into strings.
        """
        formattedResult = DBFormatter.formatDict(self, result)[0]

        if formattedResult["bool_outcome"] == 0:
            formattedResult["outcome"] = "failure"
        else:
            formattedResult["outcome"] = "success"

        del formattedResult["bool_outcome"]
        return formattedResult

    def execute(self, name, conn=None, transaction=False):
        result = self.dbi.processData(self.sql, {"name": name}, conn=conn,
                                      transaction=transaction)
        return self.formatDict(result)
