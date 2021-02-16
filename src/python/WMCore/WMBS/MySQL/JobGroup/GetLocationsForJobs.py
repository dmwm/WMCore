#!/usr/bin/env python
"""
_GetLocationForJobs_

MySQL implementation of JobGroup.GetLocationsForJobs
"""

__all__ = []

import logging

from WMCore.Database.DBFormatter import DBFormatter

from future.utils import listvalues

class GetLocationsForJobs(DBFormatter):
    """
    _GetLocationsForJobs_

    Working under the current jobSplitting assumption, namely that each jobGroup contains
    only jobs that can run at the same SET of sites, we can find the jobs the jobGroup can
    run at by selecting them from the files.
    """
    sql = """SELECT DISTINCT site_name FROM wmbs_location wl
          INNER JOIN wmbs_location_pnns wlpnn ON wlpnn.location = wl.id
          INNER JOIN wmbs_file_location wfl ON wfl.pnn = wlpnn.pnn
          INNER JOIN wmbs_job_assoc wja ON wja.fileid = wfl.fileid
          INNER JOIN wmbs_job wj ON wj.id = wja.job
          WHERE wj.jobgroup = :jobgroup"""

    def format(self, result):

        newResult = []
        # First, actually get something useful
        modifiedResult = result[0].fetchall()

        for res in modifiedResult:
            # Should only have one entry (one site_name per site)
            tmp = listvalues(res)[0]
            if not tmp in newResult:
                newResult.append(tmp)

        return newResult

    def execute(self, id, conn=None, transaction=False):
        """
        _execute_

        Execute the SQL for the given job ID and then format and return
        the result.
        """

        if id == -1:
            logging.error("JobGroup.GetLocationsForJobs got unspecified jobGroup ID")
            return []

        result = self.dbi.processData(self.sql, {"jobgroup": id}, conn=conn,
                                      transaction=transaction)

        return self.format(result)
